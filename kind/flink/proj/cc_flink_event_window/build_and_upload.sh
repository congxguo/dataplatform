mvn package

aws s3 cp target/CcFlinkEventWindow-1.0-SNAPSHOT.jar s3://flink/jars/CcFlinkEventWindow-1.0-SNAPSHOT.jar --region us-east-1 --endpoint-url http://localhost:9000 --profile minio
