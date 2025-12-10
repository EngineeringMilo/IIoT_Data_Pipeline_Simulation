import os
import json
import time
import random
import logging
from datetime import datetime, timedelta, timezone
from kafka import KafkaProducer #to connect send to kafka/redpanda
from kafka.errors import KafkaError
from dotenv import load_dotenv

load_dotenv()

# ============================================================
# Configuration
# ============================================================

BROKER = os.getenv("BROKER", "redpanda:9092")#host
TOPIC = os.getenv("TOPIC", "machine-sensors")#where sent
INTERVAL = float(os.getenv("INTERVAL", 2))
BACKFILL_COUNT = int(os.getenv("BACKFILL_COUNT", 500))
BATCH_SIZE = int(os.getenv("BATCH_SIZE", 50))

# ============================================================
# Logging
# ============================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# ============================================================
# Machines
# ============================================================

MACHINES = [
    {
        "id": "M-1001",
        "type": "CNC Mill",
        "location": "Plant 1 - Bay A",
        "sensors": {
            "temperature": (60, 5),
            "vibration": (2.1, 0.3),
            "power_load": (75, 10),
        },
    },
    {
        "id": "M-2001",
        "type": "Lathe",
        "location": "Plant 2 - Bay C",
        "sensors": {
            "temperature": (55, 4),
            "vibration": (1.5, 0.2),
            "power_load": (65, 8),
        },
    },
    {
        "id": "M-3001",
        "type": "Hydraulic Press",
        "location": "Plant 1 - Bay B",
        "sensors": {
            "temperature": (70, 6),
            "pressure": (1200, 100),
            "power_load": (80, 12),
        },
    },
]

# ============================================================
# Kafka Producer
# ============================================================
global producer
producer = None  # globale variabele

def create_producer():
    """Create a Kafka producer with retry loop."""
    while True:
        try:
            p = KafkaProducer(
                bootstrap_servers=BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                retries=5,
            )
            logging.info(f"Connected to Kafka broker at {BROKER}")
            return p
        except Exception as e:
            logging.error(f"Kafka connection failed: {e}, retrying in 5s...")
            time.sleep(5)

def get_producer():
    """Return the global producer, reconnecting if needed."""
    global producer
    if producer is None:
        producer = create_producer()
    return producer

# ============================================================
# Sensor generation
# ============================================================

def generate_sensor_readings(machine, timestamp=None):
    readings = {
        sensor: round(random.gauss(mean, std), 4)
        for sensor, (mean, std) in machine["sensors"].items()
    }
    ts = timestamp or datetime.now(timezone.utc)
    if isinstance(ts, datetime):
        ts = ts.isoformat()
    return {
        "machine_id": machine["id"],
        "machine_type": machine["type"],
        "location": machine["location"],
        "timestamp": ts,
        "readings": readings,
    }

# ============================================================
# Kafka helper
# ============================================================

def send_messages(messages):
    p = get_producer()
    for msg in messages:
        try:
            future = p.send(TOPIC, msg)
            future.get(timeout=10)
        except KafkaError as e:
            logging.error(f"Failed to send message: {e}")
            logging.info("Reconnecting Kafka producer...")
            global producer
            producer = create_producer()
    p.flush()# ensures all messages sent before continue

# ============================================================
# Backfill historical data
# ============================================================

def backfill_data():
    logging.info("Starting backfill of historical data...")

    now = datetime.now(timezone.utc)
    week_ago = now - timedelta(days=7)

    for machine in MACHINES:
        messages = []
        for i in range(BACKFILL_COUNT):
            ts = week_ago + timedelta(seconds=i * 60)
            messages.append(generate_sensor_readings(machine, timestamp=ts))

            if len(messages) >= BATCH_SIZE:
                send_messages(messages)
                messages = []

        # Flush remaining messages
        if messages:
            send_messages(messages)

    logging.info("Backfill completed.")

# ============================================================
# Realtime streaming
# ============================================================

def stream_realtime():
    logging.info("Starting realtime sensor streaming...")
    global producer
    get_producer()  # zorg dat producer actief is

    next_time = time.time()
    while True:
        for machine in MACHINES:
            msg = generate_sensor_readings(machine)
            send_messages([msg])
            logging.info(f"Sent realtime message for {machine['id']}")

        # Precise interval
        next_time += INTERVAL
        sleep_time = next_time - time.time()
        if sleep_time > 0:
            time.sleep(sleep_time)

# ============================================================
# Main
# ============================================================

if __name__ == "__main__":
    backfill_data()
    stream_realtime()