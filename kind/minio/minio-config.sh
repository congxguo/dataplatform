kubectl port-forward svc/minio 9000:9000 -n minio


# Create a profile for MinIO:
aws configure --profile minio
