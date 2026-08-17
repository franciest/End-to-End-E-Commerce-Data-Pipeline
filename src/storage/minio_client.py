"""
storage/minio_client.py

Wraps boto3's S3 client pointed at MinIO instead of AWS. Because MinIO
speaks the S3 API, this same code (just swap the endpoint/credentials)
would work against real AWS S3 later — that's the whole point of using
MinIO for local dev.

Usage:
    from storage.minio_client import get_client, ensure_bucket, upload_file

    ensure_bucket()
    upload_file("data/raw/products.csv", "raw/products.csv")
"""

import os

import boto3
from botocore.exceptions import ClientError

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_client = None


def get_client():
    """Return a singleton boto3 S3 client configured for MinIO.

    NOTE on endpoint: settings.MINIO_ENDPOINT is set to the Docker-network
    hostname (http://minio:9000) so containers (Airflow tasks) can reach it
    by service name. If you're running a script from your host machine
    (outside Docker) against the same MinIO, use http://localhost:9000
    instead — the port is published to your host, but 'minio' as a hostname
    only resolves inside the Docker network. Easiest fix: override with an
    env var when running locally, e.g.
        $env:MINIO_ENDPOINT="http://localhost:9000"; python scripts/create_buckets.py
    """
    global _client
    if _client is None:
        logger.info("Creating MinIO client", extra={"endpoint": settings.minio_endpoint})
        _client = boto3.client(
            "s3",
            endpoint_url=settings.minio_endpoint,
            aws_access_key_id=settings.minio_root_user,
            aws_secret_access_key=settings.minio_root_password,
            region_name="us-east-1",  # required by boto3 even though MinIO ignores it
        )
    return _client


def ensure_bucket(bucket_name: str = None) -> None:
    """Create the bucket if it doesn't already exist. Idempotent — safe to
    call every time the pipeline starts."""
    bucket_name = bucket_name or settings.minio_bucket
    client = get_client()
    try:
        client.head_bucket(Bucket=bucket_name)
        logger.info("Bucket already exists", extra={"bucket": bucket_name})
    except ClientError as e:
        error_code = int(e.response["Error"]["Code"])
        if error_code == 404:
            client.create_bucket(Bucket=bucket_name)
            logger.info("Bucket created", extra={"bucket": bucket_name})
        else:
            logger.error("Error checking bucket", extra={"error": str(e)})
            raise


def upload_file(local_path: str, object_key: str, bucket_name: str = None) -> None:
    """Upload a local file to MinIO under the given object key
    (e.g. 'raw/products.csv')."""
    bucket_name = bucket_name or settings.minio_bucket
    client = get_client()
    client.upload_file(local_path, bucket_name, object_key)
    logger.info(
        "File uploaded",
        extra={"local_path": local_path, "bucket": bucket_name, "key": object_key},
    )


def delete_prefix(prefix: str, bucket_name: str = None) -> int:
    """Delete every object under a prefix. Used before mirroring a fresh
    Spark write to MinIO, so 'overwrite' actually means overwrite in the
    object store — not just locally. Spark write output filenames are
    randomly generated per run, so without this, every re-run of a bronze/
    silver job leaves the previous run's file(s) sitting alongside the new
    one, silently duplicating data on the next read. Returns the number of
    objects deleted."""
    bucket_name = bucket_name or settings.minio_bucket
    client = get_client()
    keys = list_objects(prefix=prefix, bucket_name=bucket_name)
    if not keys:
        return 0
    # delete_objects takes batches of up to 1000 keys
    for i in range(0, len(keys), 1000):
        batch = keys[i:i + 1000]
        client.delete_objects(
            Bucket=bucket_name,
            Delete={"Objects": [{"Key": k} for k in batch]},
        )
    logger.info("Deleted stale objects under prefix", extra={"prefix": prefix, "count": len(keys)})
    return len(keys)


def list_objects(prefix: str = "", bucket_name: str = None) -> list[str]:
    """List object keys in the bucket under an optional prefix."""
    bucket_name = bucket_name or settings.minio_bucket
    client = get_client()
    response = client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    return [obj["Key"] for obj in response.get("Contents", [])]


def download_file(object_key: str, local_path: str, bucket_name: str = None) -> None:
    """Download a single object to a local path, creating parent dirs as
    needed."""
    bucket_name = bucket_name or settings.minio_bucket
    client = get_client()
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    client.download_file(bucket_name, object_key, local_path)
    logger.info(
        "File downloaded",
        extra={"bucket": bucket_name, "key": object_key, "local_path": local_path},
    )


def download_prefix(prefix: str, local_dir: str, bucket_name: str = None) -> list[str]:
    """Download every object under a prefix into local_dir, preserving the
    relative path structure below the prefix. Returns the list of local
    paths written. Used by the Spark sync step (Option B: Spark reads local
    files, MinIO stays out of Spark's read path entirely)."""
    bucket_name = bucket_name or settings.minio_bucket
    keys = list_objects(prefix=prefix, bucket_name=bucket_name)
    local_paths = []
    for key in keys:
        relative = key[len(prefix):].lstrip("/")
        local_path = os.path.join(local_dir, relative) if relative else os.path.join(local_dir, os.path.basename(key))
        download_file(key, local_path, bucket_name=bucket_name)
        local_paths.append(local_path)
    logger.info(
        "Prefix download complete",
        extra={"prefix": prefix, "local_dir": local_dir, "file_count": len(local_paths)},
    )
    return local_paths


def upload_dir(local_dir: str, prefix: str, bucket_name: str = None) -> list[str]:
    """Upload every file under local_dir to MinIO under prefix, preserving
    relative paths. Returns the list of object keys written. Used to mirror
    Spark's local Bronze/Silver/Gold output back into MinIO."""
    bucket_name = bucket_name or settings.minio_bucket
    uploaded_keys = []
    for root, _, files in os.walk(local_dir):
        for filename in files:
            local_path = os.path.join(root, filename)
            relative = os.path.relpath(local_path, local_dir).replace(os.sep, "/")
            object_key = f"{prefix.rstrip('/')}/{relative}"
            upload_file(local_path, object_key, bucket_name=bucket_name)
            uploaded_keys.append(object_key)
    logger.info(
        "Directory upload complete",
        extra={"local_dir": local_dir, "prefix": prefix, "file_count": len(uploaded_keys)},
    )
    return uploaded_keys