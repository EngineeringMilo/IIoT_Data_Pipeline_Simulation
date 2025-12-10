from pyflink.table import TableEnvironment, EnvironmentSettings
import time

# 1. Create Table Environment in streaming mode
env_settings = EnvironmentSettings.in_streaming_mode()
t_env = TableEnvironment.create(env_settings)

# 2. Define Redpanda/Kafka source table
t_env.execute_sql("""
CREATE TABLE machine_sensors (
    machine_id STRING,
    machine_type STRING,
    location STRING,
    ts TIMESTAMP(3),
    readings ROW<
        temperature DOUBLE,
        vibration DOUBLE,
        power_load DOUBLE
    >,
    WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'machine-sensors',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-test-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json'
)
""")

# 3. Define a print sink for testing
t_env.execute_sql("""
CREATE TABLE print_sink (
    machine_id STRING,
    machine_type STRING,
    temperature DOUBLE,
    vibration DOUBLE,
    power_load DOUBLE
) WITH (
    'connector' = 'print'
)
""")

# 4. Transform and insert into print sink
result=t_env.execute_sql("""
INSERT INTO print_sink
SELECT
    machine_id,
    machine_type,
    readings.temperature AS temperature,
    readings.vibration AS vibration,
    readings.power_load AS power_load
FROM machine_sensors
""")

print("Streaming job submitted. Container will stay alive...")

while True:
    time.sleep(60)