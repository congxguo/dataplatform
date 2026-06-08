kubectl create namespace minio

kubectl apply -f minio-storage.yaml
kubectl apply -f minio-statefulset.yaml
kubectl apply -f minio-headless.yaml
