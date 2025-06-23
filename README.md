
# Qversity Project – Cali 2025

A local data platform that implements a Lakehouse architecture (Bronze → Silver → Gold) using Docker Compose, Airflow, dbt, PostgreSQL, and FastAPI.

## 🧠 Overview

This project structures and transforms a messy mobile customer dataset into business-ready analytics using the **Medallion Architecture**:

- **Bronze Layer**: Raw data ingestion from a public JSON (S3), schema validation, and metadata enrichment.
- **Silver Layer**: Structured transformation into intermediate relational models.
- **Silver Cleaned**: Advanced data cleaning, quality validation, and normalization.
- **Gold Layer**: Final business analytics models – customer segmentation, revenue analysis, ARPU by plan, device preferences, and more.

### 📂 Business Insights

The folder [`/business_insights/`](./business_insights/) contains over **20 Markdown reports** covering:
- Device brand trends by country, operator, and plan
- Popular services and revenue-driving combinations
- Payment behavior and credit score correlation
- Customer acquisition and churn trends
- Demographic distributions (age, location, segment)
- Revenue breakdowns by region, plan, and customer type

Each insight is derived from a Gold-layer model and supports data-driven decision-making for telco strategy and optimization.

## 👤 Participant

- **Name**: Juan José Angarita Yela
- **Email**: angaritayelaj@gmail.com
---

## 🧱 Architecture

```
/
├── airflow/                   # Airflow DAGs and ETL logic
│   ├── bronze/                # Ingestion, validation, raw load
│   ├── dags/                  # Orchestration DAGs per layer
│   └── tests/                 # Integration tests for DAGs
├── app/                       # FastAPI backend to expose Gold-layer models
│   ├── api/routers/           # REST endpoints
│   ├── core/                  # SQLAlchemy models and DB setup
│   ├── db/                    # Repositories and logic
│   └── schemas/               # Pydantic schemas
├── dbt/                       # dbt project with layered models
│   └── models/
│       ├── bronze/
│       ├── silver/
│       ├── silver_cleaned/
│       └── gold/
├── business_insights/        # Markdown files with analytical summaries
├── docker-compose.yml        # Container orchestration
├── Dockerfile.airflow        # Custom Airflow image
└── practices.txt             # Project logbook and notes
```

---

## 🚀 Quick Start

### Requirements

- Docker + Docker Compose
- At least 4GB of available RAM

### Setup

```bash
# Clone the repo
git clone <repo-url>
cd qversity-data-2025-Cali-JuanJoseAngaritaYela

# Start containers
docker compose up -d
```

---

## 🔌 Access Points

- **Airflow UI**: [http://localhost:8080](http://localhost:8080) (admin/admin)
- **FastAPI Docs**: [http://localhost:8000/docs](http://localhost:8000/docs)
- **PostgreSQL**: `localhost:5432` (user: `qversity-admin`, db: `qversity`)

---

## 🛠 Common Commands

### Airflow

```bash
docker compose logs -f airflow
docker compose exec airflow airflow dags trigger bronze_ingest_customers
```

### dbt

```bash
docker compose exec airflow bash
dbt run --select bronze
dbt run --select silver.*
dbt run --select silver_cleaned.*
dbt run --select gold.*
dbt test --select silver_cleaned.*
dbt test --select gold.*
dbt docs generate
dbt docs serve
```

### FastAPI

```bash
# Swagger UI available at:
http://localhost:8000/docs
```

---

## 🧪 Development & Testing

### DAGs

1. Add new DAG in `airflow/dags/`
2. Airflow auto-detects it

### dbt Models

1. Create SQL files in `dbt/models/{layer}/`
2. Document and test them
3. Run with `dbt run`

### FastAPI Endpoints

1. Define SQLModel in `app/core/models.py`
2. Create Pydantic schema in `app/schemas`
3. Add route to `app/api/routers/gold.py`

### Tests

```bash
pytest airflow/tests/test_bronze_ingest_customers.py -vv
```

---

## 📈 Monitoring

```bash
docker compose logs -f airflow
docker compose logs -f dbt
docker compose logs -f postgres
docker compose ps
```

---

## 🧹 Cleanup

```bash
docker compose down
docker compose down -v      # ⚠️ removes volumes
docker compose down --rmi all
```
