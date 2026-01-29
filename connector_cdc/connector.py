import os
import json
import requests
from dotenv import load_dotenv

# -----------------------------------
# Load environment variables
# -----------------------------------
load_dotenv()

CONNECT_URL = os.getenv("DEBEZIUM_CONNECT_URL", "http://localhost:8083")

# -----------------------------------
# Build Debezium Connector Config
# -----------------------------------
connector_config = {
    "name": "banking-postgres-connector",
    "config": {
        "connector.class": "io.debezium.connector.postgresql.PostgresConnector",

        # ---------- Database ----------
        "database.hostname": os.getenv("POSTGRES_HOST"),
        "database.port": os.getenv("POSTGRES_PORT"),
        "database.user": os.getenv("POSTGRES_USER"),
        "database.password": os.getenv("POSTGRES_PASSWORD"),
        "database.dbname": os.getenv("POSTGRES_DB"),

        # ---------- Kafka ----------
        "topic.prefix": "banking",
        "tasks.max": "1",

        # ---------- CDC ----------
        "plugin.name": "pgoutput",
        "slot.name": "banking_slot",
        "publication.autocreate.mode": "filtered",
        "table.include.list": (
            "public.customers,"
            "public.accounts,"
            "public.transactions"
        ),

        # ---------- Data Handling ----------
        "tombstones.on.delete": "false",
        "decimal.handling.mode": "double",
        "snapshot.mode": "initial",

        # ---------- JSON (Schema-less) ----------
        "key.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "key.converter.schemas.enable": "false",
        "value.converter.schemas.enable": "false"
    }
}

# -----------------------------------
# Create Connector
# -----------------------------------
headers = {"Content-Type": "application/json"}
url = f"{CONNECT_URL}/connectors"

print("🚀 Creating Debezium Postgres connector...")

response = requests.post(
    url,
    headers=headers,
    data=json.dumps(connector_config)
)

# -----------------------------------
# Debug / Output
# -----------------------------------
if response.status_code == 201:
    print("✅ Connector created successfully!")
elif response.status_code == 409:
    print("⚠️ Connector already exists (HTTP 409)")
elif response.status_code == 400:
    print("❌ Invalid connector configuration:")
    print(response.text)
else:
    print(f"❌ Failed to create connector ({response.status_code})")
    print(response.text)
