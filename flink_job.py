import json
import time
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, EnvironmentSettings

KAFKA_BROKER = 'localhost:9092'
SOURCE_TOPIC = 'stock_tick_raw'
SINK_TOPIC = 'kline_result'
GROUP_ID = 'flink_group'

def main():
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars()

    # 创建 Table 环境
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, settings)

    # 添加 Kafka Connector JAR（本地路径）
    jar_path = "file:///D:/Tech/Python_For_Interview/pyflink_kafka_demo/libs/flink-sql-connector-kafka-3.2.0-1.18.jar"
    t_env.get_config().set("pipeline.jars", jar_path)
    print(f"JAR configured: {jar_path}")

    # 创建 Kafka 源表
    # Table API 的核心：用 SQL 定义表结构
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

    # 创建 Kafka 结果表
    # Table API 的核心：用 SQL 定义表结构
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

    # 执行插入查询
    # Table API 的核心：用 SQL 做流处理
    t_env.execute_sql("""
        INSERT INTO kafka_sink
        SELECT
            symbol,
            TUMBLE_START(proc_time, INTERVAL '1' MINUTE) AS window_start,
            TUMBLE_END(proc_time, INTERVAL '1' MINUTE) AS window_end,
            FIRST_VALUE(price) AS open_price,
            MAX(price) AS high_price,
            MIN(price) AS low_price,
            LAST_VALUE(price) AS close_price,
            CAST(SUM(volume) AS BIGINT) AS volume
        FROM kafka_source
        GROUP BY symbol, TUMBLE(proc_time, INTERVAL '1' MINUTE)
    """).wait()

if __name__ == "__main__":
    main()