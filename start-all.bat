@echo off
echo ========================================
echo Starting Kafka and MySQL only
echo ========================================

echo 1. Starting Docker containers...
docker-compose up -d kafka mysql

echo 2. Waiting for services to be ready (15 seconds)...
timeout /t 15 /nobreak >nul

echo 3. Initializing Kafka topics...
docker exec kafka kafka-topics --create --topic stock_tick_raw --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>nul
docker exec kafka kafka-topics --create --topic kline_result --bootstrap-server localhost:9092 --partitions 3 --replication-factor 1 2>nul
echo    Kafka topics ready

echo 4. Initializing MySQL table...
docker exec mysql mysql -uroot -proot123 -e "CREATE DATABASE IF NOT EXISTS stock_db;" 2>nul
docker exec mysql mysql -uroot -proot123 -e "USE stock_db; CREATE TABLE IF NOT EXISTS kline_1min (symbol VARCHAR(20), window_start TIMESTAMP, window_end TIMESTAMP, open_price DECIMAL(10,2), high_price DECIMAL(10,2), low_price DECIMAL(10,2), close_price DECIMAL(10,2), volume BIGINT, PRIMARY KEY (symbol, window_start));" 2>nul
echo    MySQL table ready

echo ========================================
echo Kafka and MySQL are ready!
echo ========================================
echo.
echo Run these commands in separate terminals:
echo   Terminal 1: python producer.py
echo   Terminal 2: python flink_job.py
echo   Terminal 3: python mysql_consumer.py
echo.