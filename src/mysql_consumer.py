import json
import mysql.connector
from kafka import KafkaConsumer

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'kline_result'
GROUP_ID = 'mysql_consumer'

MYSQL_CONFIG = {
    'host': 'localhost',
    'user': 'root',
    'password': 'root123',
    'database': 'stock_db'
}

def insert_to_mysql(data):
    conn = mysql.connector.connect(**MYSQL_CONFIG)
    cursor = conn.cursor()
    sql = """
        INSERT INTO kline_1min 
        (symbol, window_start, window_end, open_price, high_price, low_price, close_price, volume)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        open_price = VALUES(open_price),
        high_price = VALUES(high_price),
        low_price = VALUES(low_price),
        close_price = VALUES(close_price),
        volume = VALUES(volume)
    """
    cursor.execute(sql, (
        data['symbol'], data['window_start'], data['window_end'],
        data['open_price'], data['high_price'], data['low_price'],
        data['close_price'], data['volume']
    ))
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Inserted: {data['symbol']} | O:{data['open_price']} H:{data['high_price']} L:{data['low_price']} C:{data['close_price']} V:{data['volume']}")

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        group_id=GROUP_ID,
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )
    print(f"Listening to {TOPIC}...")
    for msg in consumer:
        insert_to_mysql(msg.value)

if __name__ == "__main__":
    main()