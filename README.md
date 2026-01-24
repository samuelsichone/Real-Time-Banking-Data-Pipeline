🚀 Real-Time Banking Data Pipeline

Production-style real-time data pipeline simulating modern banking transaction processing using CDC, streaming, cloud storage, transformation layers, and orchestration.

This project is designed to mirror how data engineering teams build, monitor, and debug real-world financial data pipelines.

📌 Project Overview

This pipeline ingests real-time banking transactions, streams them through Kafka using CDC (Debezium), stores raw and curated data in a lakehouse-style architecture (Bronze / Silver / Gold), and transforms it using dbt, orchestrated by Apache Airflow.

The focus is not just data flow — but reliability, observability, and failure recovery, exactly how pipelines behave in production.

🏗 Architecture
![Uploading Untitled Diagram.drawio (3).png…]()
