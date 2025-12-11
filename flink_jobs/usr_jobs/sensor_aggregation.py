import json
import time
import socket
from pyflink.common import WatermarkStrategy
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource

# Thresholds for alerts
THRESHOLDS = {
    "temperature": 100.0,
    "pressure": 1000.0,
    "power_load": 100.0,
    "vibration": 3.0
}

# Broker configuration
BROKER_HOST = "redpanda"
BROKER_PORT = 9092
KAFKA_TOPIC = "machine-sensors"

def wait_for_broker(host: str, port: int, retry_interval: float = 2.0):
    """
    Wait until the Kafka/Redpanda broker is reachable.
    """
    while True:
        try:
            with socket.create_connection((host, port), timeout=2):
                print(f"{host}:{port} is reachable. Starting Flink job...")
                return
        except Exception:
            print(f"Waiting for broker {host}:{port}...")
            time.sleep(retry_interval)

def parse_and_filter(value: str) -> str | None:
    """
    Parse JSON message, check thresholds, and return alert JSON if any exceeded.
    """
    data = json.loads(value)
    machine_id = data.get("machine_id")
    machine_type = data.get("machine_type")
    location = data.get("location")
    timestamp = data.get("timestamp")
    readings = data.get("readings", {})

    alerts = {}
    for sensor, threshold in THRESHOLDS.items():
        sensor_value = readings.get(sensor)
        if sensor_value is not None and sensor_value > threshold:
            alerts[sensor] = sensor_value

    if alerts:
        return json.dumps({
            "machine_id": machine_id,
            "machine_type": machine_type,
            "location": location,
            "alerts": alerts,
            "timestamp": timestamp
        })

    return None

def main():
    # Wait for Redpanda broker to be ready
    wait_for_broker(BROKER_HOST, BROKER_PORT)

    # Create Flink streaming environment
    env = StreamExecutionEnvironment.get_execution_environment()

    # Make sure the Flink Kafka connector JAR is mounted in the container
    env.add_jars("file:///opt/flink/lib/flink-sql-connector-kafka-3.1.0-1.18.jar")

    # Kafka source configuration
    properties = {
        "bootstrap.servers": f"{BROKER_HOST}:{BROKER_PORT}",
        "group.id": "iot-sensors"
    }

    kafka_source = KafkaSource.builder() \
        .set_topics(KAFKA_TOPIC) \
        .set_properties(properties) \
        .set_starting_offsets(KafkaOffsetsInitializer.latest()) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .build()

    # Create data stream from Kafka source
    stream = env.from_source(
        kafka_source,
        WatermarkStrategy.no_watermarks(),
        "Kafka Sensors Source"
    )

    # Parse, filter, and print alerts
    alerts = stream.map(parse_and_filter).filter(lambda x: x is not None)
    alerts.print()

    # Execute the Flink job
    env.execute("Sensor Alerts Consumer")

if __name__ == "__main__":
    main()
