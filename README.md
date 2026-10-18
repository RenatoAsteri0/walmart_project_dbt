# Walmart Data Engineering Platform
<img width="1774" height="887" alt="Arquitetura Lakehouse com Databricks e Airflow" src="https://github.com/user-attachments/assets/f386d5e1-c765-4d69-abf8-3f8f70fa6ba2" />

End-to-end data engineering project focused on building a multi-source data pipeline for retail analytics, integrating relational data and file-based data ingestion with modern data engineering tools.

The project combines **PostgreSQL, Amazon S3, Databricks, dbt, Apache Airflow and Python** to demonstrate data ingestion, lakehouse architecture, SQL transformations, data quality validation, historical tracking and workflow orchestration.

## Project Overview

This project simulates a retail data platform that consolidates data from multiple sources and prepares it for analytical consumption.

The platform includes two data source types:

1. **Relational data:** operational retail data stored in PostgreSQL and hosted on Neon.
2. **File-based data:** product review data stored in an Amazon S3 bucket and accessed through a Databricks external location.

Databricks provides the lakehouse environment for data ingestion and processing. dbt manages SQL-based transformations, data tests and historical snapshots, while Apache Airflow 3 orchestrates the main pipeline.

The project is designed to demonstrate practical data engineering concepts, including incremental ingestion, layered modeling, dependency management, data quality and pipeline reliability.

## Architecture

### High-level architecture

```text
                   DATA SOURCES
                         |
             +-----------+-----------+
             |                       |
             v                       v
      PostgreSQL (Neon)       Amazon S3 Bucket
             |                  reviews.csv
             v                       |
      Databricks Lakeflow            v
      Query-based capture     Databricks file ingestion
             |                       |
             v                       v
          Bronze             Reviews dataset
             |
             v
      dbt Silver Technical
             |
             v
      dbt Silver Business
             |
             v
       dbt Gold Models
       and Snapshots
             |
             v
     Analytics-ready Data
```

**Orchestration:** Apache Airflow 3 coordinates the main PostgreSQL-to-Databricks pipeline and the downstream dbt execution stages.

The S3 reviews ingestion is represented as an additional ingestion path. Its integration into the main Airflow DAG and its downstream transformation dependencies should be documented once those connections are confirmed.

## Technology Stack

| Technology | Purpose |
|---|---|
| PostgreSQL (Neon) | Relational source database |
| Amazon S3 | Object storage for file-based source data |
| Databricks | Data ingestion and lakehouse processing |
| Databricks Lakeflow | Query-based ingestion from PostgreSQL |
| Delta Lake | Table storage and processing in Databricks |
| dbt | SQL transformations, tests, freshness checks and snapshots |
| Apache Airflow 3 | Workflow orchestration |
| Python | Pipeline integration and supporting scripts |
| uv | Python dependency and environment management |
| Git and GitHub | Version control and project documentation |

## Data Sources

### 1. PostgreSQL — Neon
<img width="1918" height="967" alt="Screenshot 2026-10-18 103004" src="https://github.com/user-attachments/assets/2be78bb7-374e-4f2f-8b1e-209eee60fdd1" />

The primary relational source is a PostgreSQL database hosted on Neon.

The source contains operational retail entities, including:

- Orders
- Customers
- Employees
- Order items
- Products
- Stores

These entities provide the foundation for the main analytical data pipeline.

#### Ingestion strategy
<img width="1915" height="963" alt="Screenshot 2026-10-18 103320" src="https://github.com/user-attachments/assets/b80c1c65-f929-429d-8ac9-1bdfa40f06e9" />

Data is ingested into Databricks Lakeflow using **query-based capture with an `updated_timestamp` cursor**.

The configured cursor identifies records according to their update timestamp. This approach should not be confused with native transaction-log-based Change Data Capture (CDC).

The ingested data is stored in the Databricks Bronze layer for downstream transformations.

### 2. Amazon S3 — Product Reviews
<img width="1918" height="962" alt="Screenshot 2026-10-18 103407" src="https://github.com/user-attachments/assets/20e80ffd-120f-428a-a8f6-b2716793f26d" />

The project also includes file-based ingestion from an Amazon S3 bucket.

Databricks accesses the external location named `s3-rawdata`, which exposes files stored under the `rawdata` folder.

The current example includes:

`rawdata/reviews.csv`

The reviews dataset provides information about product feedback and customer satisfaction.

### Ephemeral Models with dbt

The project implements **ephemeral models** in dbt to encapsulate reusable SQL logic without creating a physical table or view in the data warehouse.

An example is `eph_orders.sql`, which prepares order-related data from the `orders_t` model. It selects the required columns, applies `DISTINCT` to the selected records, and adds a processing timestamp using `CURRENT_TIMESTAMP()`.

```sql
SELECT DISTINCT
    order_id,
    payment_method,
    order_timestamp,
    created_timestamp AS order_created_timestamp,
    updated_timestamp AS order_updated_timestamp,
    is_active AS order_is_active,
    processed_at AS order_processed_at,
    CURRENT_TIMESTAMP() AS order_gold_processed_at
FROM {{ ref('orders_t') }}
```

The model uses `{{ ref('orders_t') }}` to declare its dependency on the upstream dbt model. Because it is configured as ephemeral, dbt incorporates its SQL into downstream models as a common table expression (CTE), rather than materializing it as a separate database object.

**Benefits of this approach:**

- **Modularity:** isolates reusable transformation logic in a dedicated model.
- **Maintainability:** keeps downstream SQL models more organized.
- **Dependency management:** uses dbt's `ref()` function to track model dependencies.
- **Reduced physical objects:** avoids creating an additional table or view solely for intermediate logic.

Ephemeral models are most appropriate for lightweight transformations that do not need to be queried independently. Since their SQL is compiled into downstream models, excessive use can make generated queries more complex.

## Data Architecture

The main relational pipeline follows a medallion-inspired architecture, separating raw ingestion, technical preparation, business transformations and analytical outputs.

### Bronze — Raw Data
<img width="1726" height="804" alt="Screenshot 2026-10-18 113027" src="https://github.com/user-attachments/assets/03e0f1e7-b68b-4c50-bf1c-8ad665a0787c" />

The Bronze layer stores data ingested from the PostgreSQL source in Databricks.

Its primary responsibilities are:

- Ingest source data into the lakehouse.
- Maintain a raw representation of source entities.
- Provide the foundation for downstream transformations.
- Separate source ingestion from business modeling.

The PostgreSQL ingestion pipeline uses query-based capture with an update timestamp cursor.

### Silver Technical (`silver_t`)

The Silver Technical layer standardizes and prepares source data for downstream processing.

Its responsibilities include:

- Selecting and standardizing technical columns.
- Preparing source entities for reuse.
- Applying technical transformations.
- Establishing a consistent foundation for business logic.

### Silver Business (`silver_b`)
<img width="1746" height="906" alt="Screenshot 2026-10-18 113059" src="https://github.com/user-attachments/assets/101a03c5-e36e-4a8c-b3a1-5463beab6d0a" />

The Silver Business layer applies business-oriented transformations and combines related entities for analytical use cases.

This layer includes business models and an Operational Business Table (OBT) designed to consolidate related retail information.

Join conditions and table grain must be validated carefully to prevent unintended row multiplication and incorrect analytical results.

### Gold

The Gold layer contains analytical models and historical snapshots.

The current catalog includes objects such as:

- `dim_customers`
- `dim_employees`
- `dim_orders`
- `dim_products`
- `dim_stores`
- `fact_orders`
- `reviews`

The `dim_orders_backup_20261017` table is a backup artifact created during snapshot troubleshooting, rather than a standard analytical dimension.

Gold models are intended to provide structured data for reporting, analysis and downstream consumption.

## Pipeline Orchestration

Apache Airflow 3 orchestrates the main pipeline through a DAG that triggers the Databricks ingestion job and coordinates the dbt execution stages.

The main workflow includes:

1. Trigger the Databricks ingestion job.
2. Run dbt source freshness validation.
3. Execute Silver Technical models and tests.
4. Execute Silver Business models and tests.
5. Run dbt snapshots.
6. Execute Gold analytical models and validations.

The DAG defines dependencies between stages so that downstream processing occurs after the required upstream tasks.

The current implementation has been executed successfully end to end for the main pipeline.
<img width="1464" height="824" alt="orchestrate-graph" src="https://github.com/user-attachments/assets/66948760-39bc-4b52-8ed1-067a44a52d53" />


## Data Quality and Historical Tracking

Data quality is incorporated into the transformation workflow through dbt validation and source freshness checks.

### Source freshness

Source freshness uses the configured `updated_timestamp` field and defined warning and error thresholds.

Freshness checks help identify source data that has not been updated within the expected time window.

Because this is a learning environment with synthetic data, static source records may become stale. In production, freshness should reflect the actual update behavior and service-level expectations of the source system.

### dbt tests

dbt tests validate assumptions about transformed data and help detect issues before downstream models are consumed.

Testing should cover relevant properties such as key uniqueness, nullability, accepted values and referential integrity, according to the requirements of each model.

### Snapshots

dbt snapshots preserve historical versions of selected records.

Reliable snapshot execution depends on stable business keys, consistent change-detection logic and a target table that does not contain conflicting current versions.

When a snapshot fails, investigate source uniqueness, snapshot configuration and target-table consistency before rebuilding the affected snapshot.
<img width="1731" height="925" alt="Screenshot 2026-10-18 113448" src="https://github.com/user-attachments/assets/97905ef4-5afd-4f99-abb1-1a3948841427" />

### Operational considerations

Production implementations should additionally define monitoring, alerting, retry policies, recovery procedures and data quality expectations for each source.

## Repository Structure

The repository contains the dbt project and Airflow orchestration code. The structure below is illustrative and should be aligned with the actual directory layout.

```text
walmart_project_dbt/
├── airflow/
│   └── walmart_project/
│       ├── dags/
│       ├── models/
│       ├── snapshots/
│       ├── tests/
│       └── dbt_project.yml
├── docker-compose-yaml
├── Dockerfile
├── README.md
├── pyproject.toml
├── uv.lock
└── .gitignore
```

The repository may also contain environment-specific configuration and local runtime artifacts. Generated logs, credentials and local configuration should not be committed unless explicitly required.

## Getting Started

### Prerequisites

Before running the project, prepare:

- Python compatible with the repository's dependency configuration.
- Access to the PostgreSQL source database on Neon.
- An Amazon S3 bucket and appropriate permissions if using the reviews dataset.
- A Databricks workspace with the required ingestion configuration.
- A configured dbt adapter and target connection.
- An Airflow 3 environment with the required connections and DAG configuration.
- `uv` for dependency management, if using the current repository setup.

### 1. Clone the repository

```bash
git clone https://github.com/RenatoAsteri0/walmart_project_dbt.git
cd walmart_project_dbt
```

### 2. Set up the Python environment

```bash
uv sync
```

Activate the virtual environment according to your operating system and shell.

### 3. Configure connections and credentials

Configure the required connection details for:

- PostgreSQL (Neon)
- Databricks
- Amazon S3, if executing the reviews ingestion
- The dbt target warehouse
- Airflow connections and variables required by the DAG

Use environment variables, supported secret-management mechanisms or local configuration files that are excluded from version control.

**Never commit passwords, access tokens, private keys or production connection strings.**

### 4. Validate the dbt project

From the directory containing `dbt_project.yml`, execute:

```bash
dbt debug
dbt parse
dbt freshness
```

These commands validate the dbt configuration, parse the project and check configured source freshness.

The freshness command requires access to the configured source relations. Exact command availability can depend on the installed dbt version.

### 5. Execute the pipeline

Start the required services and configure the Databricks job, Airflow connections and variables.

Trigger the main orchestration DAG from the Airflow interface. The DAG coordinates the configured ingestion, dbt transformations, tests, snapshots and analytical models.

The S3 reviews ingestion should be executed according to its own configured workflow unless its integration into the main DAG has been implemented and verified.

## Engineering Practices Demonstrated

This project provides practical experience with:

- Multi-source data integration.
- Relational and file-based ingestion.
- Query-based incremental capture.
- Lakehouse architecture and layered data modeling.
- SQL transformations with dbt.
- Workflow orchestration with Apache Airflow 3.
- Source freshness and data quality validation.
- Historical tracking through dbt snapshots.
- Python dependency management with `uv`.
- Version control and technical documentation.
- Troubleshooting pipeline failures and Delta Lake merge conflicts.

## Project Scope and Limitations

This repository is a learning project using synthetic retail data. Its goal is to demonstrate data engineering concepts and integration across multiple tools.

It is not presented as a production-ready data platform.

A production implementation would require additional design and validation for:

- Secure credential and secret management.
- Automated deployment and CI/CD.
- Monitoring, logging and alerting.
- Retry policies and recovery procedures.
- Data contracts and explicit quality SLAs.
- Access control and governance.
- Cost management and performance optimization.
- Schema evolution and source compatibility.
- Automated validation of ingestion completeness.
- Explicit orchestration and lineage for every data source.

## Author

**Renato Asteri0**

Data Engineering | Python | SQL | dbt | Databricks | Apache Airflow

GitHub: [RenatoAsteri0](https://github.com/RenatoAsteri0)
