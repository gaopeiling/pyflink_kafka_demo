import json
import time
import random
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'stock_tick_raw'

SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA']

print("=" * 60)
print("Kafka Producer")
print(f"Broker: {KAFKA_BROKER}")
print(f"Topic: {TOPIC}")
print("=" * 60)

# 测试连接
print("Attempting to connect to Kafka...")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        max_block_ms=5000,  # 最多等待5秒
        request_timeout_ms=5000
    )
    print("✓ Connected to Kafka successfully!")
except NoBrokersAvailable:
    print("✗ ERROR: No brokers available. Please make sure Kafka is running.")
    print("  Run: docker-compose up -d kafka")
    exit(1)
except Exception as e:
    print(f"✗ ERROR: {e}")
    exit(1)

print("Starting to send messages...")
print("Press Ctrl+C to stop\n")

count = 0
try:
    while True:
        tick = {
            'symbol': random.choice(SYMBOLS),
            'price': round(random.uniform(100, 1000), 2),
            'volume': random.randint(100, 10000),
            'timestamp': int(time.time() * 1000)
        }
        future = producer.send(TOPIC, value=tick)
        count += 1

        # 等待发送确认（可选，用于调试）
        try:
            record_metadata = future.get(timeout=1)
            print(f"[{count}] ✓ Sent: {tick} | Partition: {record_metadata.partition}")
        except Exception as e:
            print(f"[{count}] ✗ Send failed: {e}")

        time.sleep(0.1)
except KeyboardInterrupt:
    print(f"\n\nStopped. Total messages sent: {count}")
finally:
    producer.close()
    print("Producer closed.")