@echo off
echo Stopping all containers...
cd /d D:\Tech\Python_For_Interview\pyflink_kafka_demo\config
docker-compose down
cd ..
echo Done!