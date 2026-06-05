import json
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings
import os

KAFKA_BROKER = 'localhost:9092'
SOURCE_TOPIC = 'stock_tick_raw'
SINK_TOPIC = 'kline_result'
GROUP_ID = 'flink_group'


def main():
    # 使用默认配置
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    # 获取项目根目录
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    jar_path = f"file:///{project_root}/libs/flink-sql-connector-kafka-3.2.0-1.18.jar"

    print(f"Loading JAR: {jar_path}")
    t_env.get_config().set("pipeline.jars", jar_path)

    print("Creating Kafka source table...")
    t_env.execute_sql(f"""
        CREATE TABLE kafka_source (
            symbol STRING,
            price DOUBLE,
            volume INT,
            ts BIGINT,
            proc_time AS PROCTIME()
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SOURCE_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'properties.group.id' = '{GROUP_ID}',
            'format' = 'json',
            'scan.startup.mode' = 'latest-offset',
            'json.ignore-parse-errors' = 'true'
        )
    """)

    print("Creating Kafka sink table...")
    t_env.execute_sql(f"""
        CREATE TABLE kafka_sink (
            symbol STRING,
            window_start TIMESTAMP(3),
            window_end TIMESTAMP(3),
            open_price DOUBLE,
            high_price DOUBLE,
            low_price DOUBLE,
            close_price DOUBLE,
            volume BIGINT
        ) WITH (
            'connector' = 'kafka',
            'topic' = '{SINK_TOPIC}',
            'properties.bootstrap.servers' = '{KAFKA_BROKER}',
            'format' = 'json'
        )
    """)

    print("Executing query...")
    t_env.execute_sql("""
                      INSERT INTO kafka_sink
                      SELECT symbol,
                             TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
                             TUMBLE_END(proc_time, INTERVAL '1' MINUTE)   AS window_end,
                             FIRST_VALUE(price)                           AS open_price,
                             MAX(price)                                   AS high_price,
                             MIN(price)                                   AS low_price,
                             LAST_VALUE(price)                            AS close_price,
                             CAST(SUM(volume) AS BIGINT)                  AS volume
                      FROM kafka_source
                      GROUP BY symbol, TUMBLE(proc_time, INTERVAL '1' MINUTE)
                      """).wait()


if __name__ == "__main__":
    main()