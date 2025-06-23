# Qversity Project – Cali 2025

Una plataforma local de datos que implementa arquitectura de tipo Lakehouse (Bronze → Silver → Gold) utilizando Docker Compose, Airflow, DBT, PostgreSQL y FastAPI.

## Arquitectura

Este proyecto sigue el patrón Medallion (Bronze, Silver, Gold):

- **Bronze Layer**: Ingesta de datos crudos desde una fuente JSON pública (S3), validación con JSON Schema y enriquecimiento con metadatos.
- **Silver Layer**: Transformación estructurada de los datos crudos en modelos relacionales intermedios.
- **Silver Cleaned**: Limpieza avanzada, validación de calidad de datos y normalización.
- **Gold Layer**: Modelos analíticos listos para negocio, segmentaciones, agregaciones y vistas para dashboards o APIs.

## Estructura del Proyecto

```
/
├── airflow/                   # DAGs de Airflow y lógica ETL por capa
│   ├── bronze/                # Extracción, validación y carga inicial
│   ├── dags/                  # DAGs para cada capa
│   ├── tests/                 # Pruebas de integración para DAGs
├── app/                       # Backend en FastAPI para exponer la capa Gold
│   ├── api/routers/           # Endpoints REST para modelos gold
│   ├── core/                  # Configuración de DB y modelos SQLAlchemy
│   ├── db/                    # Repositorios y utilidades
│   ├── schemas/               # Esquemas Pydantic
│   └── main.py                # Punto de entrada de la API
├── dbt/                       # Proyecto DBT con modelos SQL por capa
│   └── models/
│       ├── bronze/
│       ├── silver/
│       ├── silver_cleaned/
│       └── gold/
├── docker-compose.yml         # Orquestación de contenedores
├── Dockerfile.airflow         # Imagen de Airflow custom
└── practices.txt              # Bitácora y notas del equipo
```

## Instrucciones Rápidas

### Requisitos

- Docker + Docker Compose
- Al menos 4GB de RAM disponibles

### Inicialización

```bash
# Clona el repositorio
git clone <repo-url>
cd qversity-data-2025-Cali-JuanJoseAngaritaYela

# Inicia los servicios
docker compose up -d
```

## Puntos de Acceso

- **Airflow UI**: http://localhost:8080 (admin/admin)
- **API FastAPI**: http://localhost:8000/docs
- **PostgreSQL**: `localhost:5432` (user: `qversity-admin`, db: `qversity`)

## Comandos Frecuentes

### Airflow

```bash
docker compose logs -f airflow
docker compose exec airflow airflow dags trigger bronze_ingest_customers
```

### DBT

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
# Swagger UI
http://localhost:8000/docs
```

## Desarrollo

### DAGs

1. Crear archivo en `airflow/dags/`
2. Airflow lo detectará automáticamente

### Modelos DBT

1. SQL en `dbt/models/{capa}/`
2. Documentación + tests
3. Ejecutar `dbt run`

### Endpoints FastAPI

1. Modelo en `app/core/models.py`
2. Esquema en `app/schemas`
3. Ruta en `app/api/routers/gold.py` o `gold_auto.py`

## Pruebas

```bash
pytest airflow/tests/test_bronze_ingest_customers.py -vv
```

## Monitoreo

```bash
docker compose logs -f airflow
docker compose logs -f dbt
docker compose logs -f postgres
docker compose ps
```

## Limpieza

```bash
docker compose down
docker compose down -v      # ⚠️ borra volúmenes
docker compose down --rmi all
```
