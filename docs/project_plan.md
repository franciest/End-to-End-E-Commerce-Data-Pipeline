# End-to-End Data Engineering Pipeline – Project Plan

## 1. Objective
Build a production-inspired End-to-End Data Engineering Pipeline demonstrating modern
data engineering practices using batch and streaming sources. Suitable for GitHub
portfolio, resume, Data Engineer interviews, and hands-on learning of production tools.

## 2. Tech Stack
| Category | Tool |
|---|---|
| Programming | Python 3.12 |
| Data Processing | PySpark (JVM-based — requires Java 11/17) |
| Orchestration | Apache Airflow |
| Streaming | Apache Kafka |
| Object Storage | MinIO (S3-compatible) |
| Database | PostgreSQL |
| Transformation | dbt |
| Dev Tools | Docker, Docker Compose, Git, VS Code, JupyterLab |
| Source Simulation | FastAPI |
| Testing | pytest |
| Visualization | Power BI (Windows required, or VM) |
| Future Cloud Extension | Databricks CE, AWS Glue, Azure Data Factory |

## 3. Dataset
Source: Brazilian-e-commerce-style dataset (CSV).
Tables: customers, orders, order_items, order_payments, order_reviews,
products, sellers, geolocation.
**Actual size (verified):** ~1M orders, ~2.2M order_items, ~1.15M order_payments,
~933K order_reviews, ~1M customers, 2K products, 500 sellers, 11.5K geolocation
≈ **~6.2M rows total** across all tables.

## 4. Simulated Source Systems (REVISED — FTP/SFTP removed)
FTP/SFTP was dropped: for 2,000 / 500-row tables, standing up an SFTP server,
user/key management, and polling logic added infra cost without teaching
much beyond "read a file." Replaced with MinIO as the universal landing zone —
more directly relevant to modern cloud data engineering (S3/object storage).

| Pattern | Method | Tables |
|---|---|---|
| Batch | CSV → MinIO | products, sellers, order_reviews |
| API | FastAPI → MinIO | customers, geolocation |
| Streaming | Kafka → MinIO | orders, order_items, order_payments |

## 5. Pipeline Architecture (REVISED)
```
                    Source Systems
      ┌────────────────┼────────────────┐
   Batch CSV        FastAPI            Kafka
      └────────────────┼────────────────┘
                        ▼
                MinIO (Raw Zone)
                        ▼
                 PySpark Bronze
                        ▼
                 PySpark Silver
                        ▼
                  PySpark Gold
                        ▼
                   PostgreSQL
                        ▼
                       dbt
                        ▼
                    Power BI
```

## 6. Project Structure
```
end_to_end_data_engineering_pipeline/
├── src/
│   ├── airflow/  config/  dbt/  ingestion/  kafka/
│   ├── processing/  spark/  storage/  utils/
├── docker/          # airflow/ kafka/ minio/ postgres/
├── scripts/         # create_tables.py, load_sample_data.py,
│                     # reset_database.py, create_buckets.py
├── docs/            # architecture.md, architecture.png,
│                     # data_flow.drawio, er_diagram.png, api.md
├── data/            # raw/ bronze/ silver/ gold/
├── notebooks/  tests/  logs/
├── docker-compose.yml  requirements.txt  README.md
├── .gitignore  .env  main.py
```

## 7. Medallion Architecture
RAW (original data) → BRONZE (minimal cleaning) → SILVER (business-ready
cleaned data) → GOLD (analytics-ready tables)

## 8. Development Roadmap (REVISED — demo-driven)
Every phase ends with something concretely runnable/demoable — never a phase
that only "compiles."

| Phase | Content | Demo at end of phase | Status |
|---|---|---|---|
| 1 | Environment setup (Python, Docker, Postgres, Java 17, PySpark) | Python connects to PostgreSQL | Done |
| 2 | Project structure | — | Done |
| 3 | Docker services (Postgres, MinIO, Kafka, Airflow) | All containers healthy; MinIO console + Airflow UI reachable | In progress |
| 4 | Configuration (logging, Postgres conn, MinIO conn, Kafka conn, FastAPI config) | Python uploads a CSV into MinIO | Pending |
| 5 | Ingestion: Batch (CSV→MinIO), API (FastAPI→MinIO), Streaming (Kafka→MinIO) | FastAPI returns customer JSON; Kafka producer sends & consumer receives messages | Pending |
| 6 | PySpark ETL: Raw→Bronze→Silver→Gold | PySpark transforms sample data end-to-end | Pending |
| 7 | PostgreSQL loading | Gold tables loaded into PostgreSQL | Pending |
| 8 | Airflow orchestration | Full DAG runs successfully | Pending |
| 9 | dbt modeling (staging, marts, tests, docs) | `dbt build` succeeds | Pending |
| 10 | Power BI dashboard | Dashboard refreshes live from PostgreSQL | Pending |
| 11 (optional) | Cloud version (Databricks CE / AWS Glue / ADF) | — | Future |

**Java note:** PySpark is JVM-based. Flow: `Java 17 → PySpark → JVM → Spark
Driver → Executors`. Already installed on this machine (Windows).

## 9. Engineering Principles
Every module includes: logging, exception handling, config from `.env`,
type hints, docstrings, unit tests where practical, clear function names,
single-responsibility modules, reusable components.

## 10. Git Strategy
One feature per commit (e.g. "Add PostgreSQL connection", "Add Kafka producer",
"Create Spark Bronze pipeline", "Add Airflow DAG").

## 11. Coding Standards
PEP 8. No hard-coded paths/secrets — all config via `.env`. Single-responsibility
modules. Comments only where they add understanding beyond the code itself.

## 12. Learning Goals
End-to-end batch pipeline · real-time streaming simulation (Kafka) · object
storage concepts (MinIO/S3) · large-scale transforms (PySpark) · Bronze/Silver/Gold
modeling · workflow orchestration (Airflow) · warehouse modeling (dbt) ·
dashboards (Power BI) · how these map to Databricks/AWS Glue/Azure Data Factory.

## 13. Guiding Principle
Each technology has exactly one clear purpose — no complexity added for its
own sake:
- FastAPI → API ingestion
- Kafka → streaming ingestion
- MinIO → object storage
- PySpark → scalable transformation
- Airflow → orchestration
- dbt → warehouse modeling
- Power BI → consumption/dashboarding
