@echo off
echo ========================================
echo Flink + Kafka + MySQL Pipeline
echo ========================================

cd /d D:\Tech\Python_For_Interview\pyflink_kafka_demo\config

echo [1/4] Cleaning old Kafka data...
docker-compose down -v 2>nul
echo   Cleanup done

echo [2/4] Starting Docker containers...
docker-compose up -d
timeout /t 15 /nobreak >nul

echo [3/4] Creating Kafka topics...
docker exec kafka kafka-topics --create --topic stock_tick_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>nul
docker exec kafka kafka-topics --create --topic kline_result --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>nul

echo [4/4] Initializing MySQL table...
docker exec mysql mysql -uroot -proot123 -e "CREATE DATABASE IF NOT EXISTS stock_db;" 2>nul
docker exec mysql mysql -uroot -proot123 -e "USE stock_db; CREATE TABLE IF NOT EXISTS kline_1min (symbol VARCHAR(20), window_start TIMESTAMP, window_end TIMESTAMP, open_price DECIMAL(10,2), high_price DECIMAL(10,2), low_price DECIMAL(10,2), close_price DECIMAL(10,2), volume BIGINT, PRIMARY KEY (symbol, window_start));" 2>nul

echo.
echo ========================================
echo READY! Run:
echo   python src/producer.py
echo   python src/flink_job.py
echo   python src/mysql_consumer.py
echo ========================================
pause