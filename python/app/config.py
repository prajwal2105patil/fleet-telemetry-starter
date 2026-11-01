"""All configuration comes from environment variables (12-factor style)."""
import os

DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "fleet")
DB_USER = os.getenv("DB_USER", "fleet")
DB_PASSWORD = os.getenv("DB_PASSWORD", "fleet")
DB_DSN = f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASSWORD} connect_timeout=3"

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092")
TOPIC_RAW = "telemetry.raw"
TOPIC_DLQ = "telemetry.dlq"
TOPIC_ALERTS = "alerts.out"
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "telemetry-writer")

OVERSPEED_KMPH = float(os.getenv("OVERSPEED_KMPH", "100"))
OVERSPEED_MIN_PINGS = int(os.getenv("OVERSPEED_MIN_PINGS", "3"))
