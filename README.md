🚀 Real-Time Banking Data Pipeline

Production-style real-time data pipeline simulating modern banking transaction processing using CDC, streaming, cloud storage, transformation layers, and orchestration.

This project is designed to mirror how data engineering teams build, monitor, and debug real-world financial data pipelines.

📌 Project Overview

This pipeline ingests real-time banking transactions, streams them through Kafka using CDC (Debezium), stores raw and curated data in a lakehouse-style architecture (Bronze / Silver / Gold), and transforms it using dbt, orchestrated by Apache Airflow.

The focus is not just data flow — but reliability, observability, and failure recovery, exactly how pipelines behave in production.

🏗 Architecture
![Uploading Untitled Diagram.drawio (3).png…]()
🛠 Tech Stack
Category	Tools
Language	Python
Streaming	Apache Kafka
CDC	Debezium
Storage	S3 / MinIO
Transformation	dbt
Orchestration	Apache Airflow
Data Warehouse	Snowflake / Postgres
Containerization	Docker
CI/CD	GitHub Actions
Visualization	Power BI / Metabase
🧠 Key Engineering Concepts Demonstrated

Change Data Capture (CDC)

Event-driven streaming pipelines

Bronze / Silver / Gold data modeling

Fault-tolerant ingestion

Retry & recovery strategies

Data quality enforcement

Production logging & debugging

🔄 Data Flow (Bronze → Silver → Gold)
🥉 Bronze Layer (Raw)

Stores unaltered events

Schema-on-read

Replayable for recovery

Source of truth

🥈 Silver Layer (Cleaned)

Deduplicated records

Standardized schemas

Validated fields

Business-safe data

🥇 Gold Layer (Analytics)

Aggregated metrics

KPI-ready tables

Optimized for BI tools
