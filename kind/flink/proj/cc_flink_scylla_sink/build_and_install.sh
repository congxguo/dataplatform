mvn install -DskipTests

cd ../cc_flink_event_window

mvn package -DskipTests

aws s3 cp target/flink-kafka-scylla-sink-job.jar \
  s3://flink/jars/flink-kafka-scylla-sink-job.jar \
  --region us-east-1 \
  --endpoint-url http://localhost:9000 \
  --profile minio

cd -

kubectl apply -f flink-job.yaml -n flink


