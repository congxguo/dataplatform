docker build -t flink-iceberg:1.20 -f Dockerfile .


kind load docker-image flink-iceberg:1.20 --name dataplatform
