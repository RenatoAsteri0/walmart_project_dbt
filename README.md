# Walmart Data Engineering Project

End-to-end data engineering project based on the Walmart Data Engineering
project by Ansh Lamba.

This project is being implemented and adapted as a hands-on learning
project focused on building a modern data pipeline using PostgreSQL,
Databricks, dbt and Apache Airflow.

> 🚧 Project status: In Progress

---

## 📌 Project Overview

The goal of this project is to build an end-to-end data pipeline that
ingests data from a PostgreSQL transactional database, processes it using
Databricks and dbt, and organizes the data into different layers following
the Medallion Architecture.

The project also explores incremental data ingestion, data transformation,
data quality, orchestration and analytical data modeling.

The original project uses the following main technologies:

- PostgreSQL
- Databricks
- dbt
- Apache Airflow
- AWS S3

This implementation adapts the original project to a personal learning
environment.

---

## 🏗️ Architecture

The target architecture for this project is:

PostgreSQL
    ↓
Data Ingestion / CDC
    ↓
Databricks
    ↓
Bronze Layer
    ↓
dbt
    ↓
Silver Technical
    ↓
Silver Business
    ↓
Gold
    ↓
Analytics / BI

Apache Airflow will be used to orchestrate the pipeline.

---

## 🥉 Bronze Layer

The Bronze layer is responsible for storing the data ingested from the
source system with minimal transformation.

Current source:

- PostgreSQL / Neon

Current tables:

- customers
- employees
- order_items
- orders
- products
- stores

---

## 🥈 Silver Layer

The Silver layer is responsible for cleaning, transforming and preparing
the data for analytical use.

### Silver Technical

The Silver Technical layer contains technical transformations such as:

- data filtering
- incremental processing
- timestamp handling
- data standardization
- technical metadata

Current dbt models:

- `customers_t`
- `employees_t`
- `order_items_t`
- `orders_t`
- `products_t`
- `stores_t`

> 🚧 This layer is currently under development.

### Silver Business

The Silver Business layer will contain business-oriented transformations
and structures derived from the Silver Technical layer.

> 🚧 Not implemented yet.

---

## 🥇 Gold Layer

The Gold layer will contain analytical models designed for business
consumption.

The planned implementation includes dimensional modeling and analytical
structures such as fact and dimension tables.

> 🚧 Not implemented yet.

---

## 🔄 Incremental Processing

The project explores incremental data processing using dbt.

Instead of rebuilding an entire dataset on every execution, incremental
models identify records that have been updated since the previous
processing.

The current implementation uses fields such as:

- `updated_timestamp`
- `unique_key`

This approach is being used as part of the study of incremental pipelines
and will later be compared with CDC-based ingestion.

---

## 🧪 Data Quality

Data quality and validation are part of the planned pipeline.

The project will explore:

- dbt tests
- source freshness
- primary key validation
- null checks
- referential integrity
- pipeline validation

> 🚧 Under development.

---

## ⚙️ Orchestration

Apache Airflow will be used to orchestrate the different stages of the
pipeline.

The planned workflow includes:

1. Source ingestion
2. Data freshness validation
3. Bronze processing
4. Silver Technical transformations
5. Silver Business transformations
6. Gold transformations
7. Data quality checks

> 🚧 Airflow orchestration is planned for the next stages of the project.

---

## 🛠️ Technologies

| Technology | Purpose |
|------------|---------|
| PostgreSQL / Neon | Source transactional database |
| Databricks | Data platform and processing |
| dbt | Data transformation and testing |
| Apache Airflow | Pipeline orchestration |
| Git / GitHub | Version control |
| AWS S3 | File-based data ingestion |

---

## 📂 Project Structure

```text
walmart_project_dbt/
│
├── walmart_project/
│   ├── analyses/
│   ├── macros/
│   ├── models/
│   │   └── source/
│   ├── seeds/
│   ├── snapshots/
│   ├── tests/
│   ├── dbt_project.yml
│   └── profiles.yml
│
├── src/
│   └── walmart_project_dbt/
│
├── .gitignore
├── pyproject.toml
├── README.md
└── uv.lock