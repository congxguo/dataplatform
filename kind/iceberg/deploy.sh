kubectl create namespace iceberg
kubectl apply -f iceberg-rest-catalog.yaml

aws s3api create-bucket --bucket iceberg-warehouse --region us-east-1 --endpoint-url http://localhost:9000 --profile minio
