import json
import time
import socket
from pyflink.common import WatermarkStrategy,Types
from pyflink.common.serialization import SimpleStringSchema
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.kafka import KafkaOffsetsInitializer, KafkaSource
from pyflink.datastream.functions import AggregateFunction
from pyflink.datastream.window import Time, TumblingProcessingTimeWindows, SlidingProcessingTimeWindows

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

def flatten_readings(record_str):
    record=json.loads(record_str)
    readings = record.get("readings", {})
    machine_id = record.get("machine_id")
    timestamp = record.get("timestamp")
    for sensor_name, value in readings.items():
        yield (machine_id, sensor_name, float(value), timestamp)

# AggregateFunction to compute sum, count, min, max
class StatsAgg(AggregateFunction):

    def create_accumulator(self):
        # accumulator: (sum, count, min, max)
        return (0.0, 0, float('inf'), float('-inf'))

    def add(self, value, accumulator):
        val = value[2]
        s, c, mn, mx = accumulator
        s += val
        c += 1
        mn = min(mn, val)
        mx = max(mx, val)
        return (s, c, mn, mx)

    def get_result(self, accumulator):
        s, c, mn, mx = accumulator
        avg = s / c if c > 0 else 0
        return (avg, mn, mx, c)

    def merge(self, a, b):
        s1, c1, mn1, mx1 = a
        s2, c2, mn2, mx2 = b
        return (s1+s2, c1+c2, min(mn1, mn2), max(mx1, mx2))

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

    # Flatten the readings
    flat_stream = stream.flat_map(flatten_readings, 
                                  output_type=Types.TUPLE([
                                      Types.STRING(),  # machine_id
                                      Types.STRING(),  # sensor_name
                                      Types.FLOAT(),   # value
                                      Types.STRING()   # timestamp
                                  ]))

    # Tumbling window: 1 minute
    tumbling_agg = (
        flat_stream
        .key_by(lambda x: (x[0], x[1]))  # key: machine_id, sensor_name
        .window(TumblingProcessingTimeWindows.of(Time.minutes(1)))
        .aggregate(StatsAgg(),
                   output_type=Types.TUPLE([
                       Types.FLOAT(),  # avg
                       Types.FLOAT(),  # min
                       Types.FLOAT(),  # max
                       Types.INT()     # count
                   ]))
    )
    tumbling_agg.map(lambda x: f"TUMBLING WINDOW AGG: {x}").print()

    # Sliding window: 1 minute sliding every 30s
    sliding_agg = (
        flat_stream
        .key_by(lambda x: (x[0], x[1]))
        .window(SlidingProcessingTimeWindows.of(Time.minutes(1), Time.seconds(30)))
        .aggregate(StatsAgg(),
                   output_type=Types.TUPLE([
                       Types.FLOAT(),  # avg
                       Types.FLOAT(),  # min
                       Types.FLOAT(),  # max
                       Types.INT()     # count
                   ]))
    )
    sliding_agg.map(lambda x: f"SLIDING WINDOW AGG: {x}").print()
    alerts = stream.map(parse_and_filter).filter(lambda x: x is not None)
    #alerts.print()

    env.execute("Sensor Window Aggregations")

if __name__ == "__main__":
    main()
