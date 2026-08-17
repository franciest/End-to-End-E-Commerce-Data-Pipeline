"""
storage/postgres_client.py

Thin wrapper around SQLAlchemy for connecting to the Postgres warehouse.
Keeps engine creation in one place so connection pooling settings are
consistent everywhere, and gives callers a context-managed session instead
of making them manage connect/close themselves.

Usage:
    from storage.postgres_client import get_engine, session_scope

    engine = get_engine()

    with session_scope() as session:
        session.execute(text("SELECT 1"))
"""

from contextlib import contextmanager

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from config.settings import settings
from utils.logger import get_logger

logger = get_logger(__name__)

_engine = None
_SessionLocal = None


def get_engine():
    """Return a singleton SQLAlchemy engine. pool_pre_ping checks the
    connection is alive before handing it out, which avoids the classic
    'connection was closed by the server' error after idle periods."""
    global _engine
    if _engine is None:
        logger.info(
            "Creating Postgres engine",
            extra={"host": settings.postgres_host, "db": settings.postgres_db},
        )
        _engine = create_engine(settings.postgres_url, pool_pre_ping=True)
    return _engine


def get_session_factory():
    global _SessionLocal
    if _SessionLocal is None:
        _SessionLocal = sessionmaker(bind=get_engine())
    return _SessionLocal


@contextmanager
def session_scope():
    """Provide a transactional session: commits on success, rolls back and
    re-raises on error, always closes. Prevents connections leaking if a
    caller forgets to close them."""
    session = get_session_factory()()
    try:
        yield session
        session.commit()
    except Exception:
        session.rollback()
        logger.exception("Session rolled back due to an error")
        raise
    finally:
        session.close()


def check_connection() -> bool:
    """Quick health check — used by our Phase 4 demo and by Airflow tasks
    that want to fail fast if the DB isn't reachable."""
    try:
        with session_scope() as session:
            session.execute(text("SELECT 1"))
        logger.info("Postgres connection OK")
        return True
    except Exception as e:
        logger.error("Postgres connection failed", extra={"error": str(e)})
        return False
