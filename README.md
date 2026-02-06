🏦 Real-Time Banking Transactions Pipeline
📌 Overview

This project implements a real-world, end-to-end data engineering pipeline for banking transactions.
It ingests real-time CDC events, persists them as partitioned Parquet files, and transforms them into analytics-ready dimensional models using dbt and Snowflake, orchestrated by Airflow.

The goal is to demonstrate production-style data engineering practices, including streaming ingestion, historical tracking (SCD Type 2), containerization, and orchestration.

🏗️ Architecture
<div align="center">
  <img src="./diagram.png" />
</div>


🔧 Tech Stack

PostgreSQL – Source transactional banking database

Apache Kafka – CDC event streaming

Python – Kafka consumers & Parquet data writer

Parquet + S3/Object Storage – Partitioned data lake storage

Apache Airflow – Workflow orchestration & scheduling

Snowflake – Cloud analytics data warehouse

dbt – Data transformations & dimensional modeling

Docker – Containerized execution environment

Power BI (DirectQuery) – Real-time analytics dashboards

📥 Data Pipeline Architecture
🔄 Ingestion Layer

Banking data captured from PostgreSQL via CDC.

Events streamed through Kafka topics:

transactions

customers

accounts

Python consumers buffer and write data into:

Partitioned Parquet files

Stored in S3/Object Storage

Partition structure:

entity/year/month/day/hour/batch.parquet

⏱️ Orchestration (Airflow)

Airflow DAGs manage the full pipeline:

Load Parquet files from S3 → Snowflake RAW layer

Trigger dbt transformations

Execute snapshots & marts

Handle retries and scheduling

❄️ Snowflake Data Modeling (dbt)
1️⃣ RAW Layer

Data ingested directly from S3

Represents near-source structured tables

2️⃣ Staging Layer (stg_)

Data cleaning & type casting

Naming standardization

Transformations

stg_transactions

stg_customers

stg_accounts

3️⃣ Dimensional Models

Built using dbt best practices:

Dimension Tables

dim_customers

dim_accounts

Fact Tables

fact_transactions

Includes:

Historical tracking via SCD Type 2

Analytics-optimized schema

Business-ready datasets

📊 Analytics Layer – Power BI

Power BI connects to Snowflake using DirectQuery

Enables near real-time dashboards

No data duplication

Supports operational and analytical reporting

✅ Project Completion Status

✔ Real-time CDC ingestion
✔ Kafka streaming pipeline
✔ Parquet data lake layer
✔ Airflow orchestration
✔ Snowflake RAW ingestion
✔ dbt staging models
✔ SCD Type 2 snapshots
✔ Dimensional models (Customers & Accounts)
✔ Fact transactions model
✔ Power BI DirectQuery dashboards

🎯 Outcome

This project demonstrates a full enterprise-grade modern data stack, covering:

Real-time data engineering

Lakehouse architecture

Analytics engineering with dbt

Workflow orchestration

Dimensional modeling

Production-style containerized systems

Business intelligence delivery
