# Production-like ETL Pipeline (CSV → PostgreSQL → Airflow)
![Tests](https://github.com/vbit85-cpu/data-pipeline-project/actions/workflows/tests.yml/badge.svg)

## Overview
This project provides a complete, production-inspired ETL pipeline that ingests CSV files, applies data quality checks, transforms the data, and loads it into PostgreSQL. 
It is built on a modular, multi-layer architecture orchestrated with Airflow.

Key features:
- YAML-driven pipeline configuration
- Dependency-aware execution
- Idempotent staging layer
- Data quality validation
- Audit tracking
- Airflow orchestration (Docker)

---

## Architecture Principles

### Data Flow
CSV → validation → bad records → staging → core → audit + data quality

### Architecture Diagram

```mermaid
flowchart TD
    A[CSV Files] --> B[Extract & Validation]
	
    B -->|invalid rows| C[Bad Records Storage]
	
    B -->|valid rows| D[Staging Layer (stg_*)]
    D -->|batch_id + source_row_number| E[Idempotent Load<br/>ON CONFLICT DO NOTHING]
	
    E --> F[Core Layer]
	
    F --> G[FK Mapping<br/>source_id → id]
	
    G --> H[Audit Tables<br/>etl_batches]
    G --> I[Data Quality<br/>etl_quality_checks]
	
    J[Airflow DAG] --> B
    J --> D
    J --> F
```

---

## Layers

### Staging (stg_*)
- append-only
- stores normalized raw data
- all columns stored as TEXT
- includes metadata:
  - batch_id
  - source_file
  - source_row_number
  - raw_record 
- loaded using COPY + temp table + ON CONFLICT DO NOTHING
- guarantees idempotency

### Core
- normalized relational schema
- typed columns (INT, FLOAT, etc.)
- surrogate keys (id)
- business keys (source_id)
- FK relationships
- UPSERT loading

---

## YAML-driven Configuration

All pipeline behavior is defined in:

configs/mappings.yaml

Includes:
- file paths
- target tables
- primary keys
- column mappings (source → normalized)
- dependencies
- transformations

### Example Mapping

```yaml
orders:
    path: orders.csv
    table: orders
    primary_key: id
    depends_on:
      - entities
      - agents
    columns:
      source_id:
        source: id
        type: int
        required: true
      ent_id:
        type: int
        required: true
      order_qty:
        type: float
        required: false
      price:
        type: float
        required: false
      order_sum:
        type: float
        required: true
      ag_id:
        type: int
        required: true
      ag_ch_id:
        type: int
        required: true
```

Mapping logic:

orders.ent_id → entities.source_id → entities.id  
orders.ag_id → agents.source_id → agents.id

---

## Dependency Graph

- built from YAML
- topological sorting
- cycle detection
- ensures correct load order

entities → orders
agents → orders

---

## Idempotent Staging Load

Implemented using:
- temporary tables
- INSERT ... ON CONFLICT DO NOTHING
- unique constraint:
  (batch_id, source_file, source_row_number)

Result:
- safe retries
- no duplicate data

---

## Core Loading Example (SQL)

The core layer is populated from staging tables using FK mapping and UPSERT logic.

Example: loading `orders` with references to `entities` and `agents`

```sql
WITH latest AS (
            SELECT DISTINCT ON (NULLIF(source_id,'')::double precision::int)
                source_id,
                ent_id,
                order_qty,
                price,
                order_sum,
                ag_id,
                ag_ch_id
            FROM stg_orders
            WHERE NULLIF(source_id,'') IS NOT NULL
              AND batch_id = %s
            ORDER BY NULLIF(source_id,'')::double precision::int, loaded_at DESC, stg_id DESC
        )
        INSERT INTO orders (
            source_id,
            ent_id,
            order_qty,
            price,
            order_sum,
            ag_id,
            ag_ch_id
        )
        SELECT
            NULLIF(l.source_id,'')::double precision::int,
            e.id AS ent_id,
            NULLIF(l.order_qty,'')::double precision,
            NULLIF(l.price,'')::double precision,
            NULLIF(l.order_sum,'')::double precision,
            parent_ag.id AS ag_id,
            child_ag.id AS ag_ch_id
        FROM latest l
        JOIN entities e
            ON NULLIF(l.ent_id,'')::double precision::int = e.source_id
        JOIN agents parent_ag
            ON NULLIF(l.ag_id,'')::double precision::int = parent_ag.source_id
        JOIN agents child_ag
            ON NULLIF(l.ag_ch_id,'')::double precision::int = child_ag.source_id
           AND child_ag.parent_id = parent_ag.id
        ON CONFLICT (source_id) DO UPDATE
        SET
            ent_id = EXCLUDED.ent_id,
            order_qty = EXCLUDED.order_qty,
            price = EXCLUDED.price,
            order_sum = EXCLUDED.order_sum,
            ag_id = EXCLUDED.ag_id,
            ag_ch_id = EXCLUDED.ag_ch_id;
```
Explanation:

stg_orders contains raw/normalized data from CSV
entities and agents are core tables with surrogate keys (id)
FK mapping is resolved using source_id
ON CONFLICT ensures idempotent updates
batch_id isolates data per pipeline run

---

## Data Quality

Checks implemented:
- File exists
- File not empty
- Schema validation
- Row-level validation
- Bad records tracking
- Duplicate detection
- Missing references
- Invalid relationships

Severity:
- warning → pipeline continues
- critical → pipeline fails

---

## Audit

Tracked in etl_batches:
- batch_id
- started_at
- finished_at
- status
- error_message

Idempotent behavior supported.

---

## Airflow

- Docker-based
- LocalExecutor
- Dynamic DAG with dependency-aware execution
- Task per table

---

## Testing

Includes:
- YAML contract validation
- dependency graph validation
- mapping consistency checks

---

## How to Run

```bash

   docker compose up airflow-init
   docker compose up -d

   python scripts/pipeline.py

   Idempotency de test :
   BATCH_ID=11111111-1111-1111-1111-111111111111 python scripts/pipeline.py
```

---

## Folder Structure

configs/        # YAML mappings
scripts/        # ETL logic
tests/          # pytest tests
logs/           # ETL logs
dags/           # DAG definitions
data/           # folder with csv files

---
## Limitations

- Basic CI with pytest only
- No deployment pipeline yet
- Batch-based processing (no incremental loading)
- Limited monitoring
- Local Airflow setup
- Staging stores all values as TEXT
- Type casting handled in core layer

---

## Tech Stack

- Python (pandas, psycopg2)
- PostgreSQL
- Apache Airflow
- Docker
- YAML

---

## Project Purpose

This project is designed as a portfolio-grade ETL system demonstrating:

- Data engineering best practices
- Pipeline reliability
- Clean architecture
- Real-world patterns
