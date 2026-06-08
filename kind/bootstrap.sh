#### prepare storage dir for kind clusters
rm -rf data/kind-worker-1
rm -rf data/kind-worker-2
rm -rf data/kind-worker-3

mkdir -p data/kind-worker-1/scylla
mkdir -p data/kind-worker-2/scylla
mkdir -p data/kind-worker-3/scylla

mkdir -p data/kind-worker-1/minio
mkdir -p data/kind-worker-2/minio
mkdir -p data/kind-worker-3/minio

mkdir -p data/kind-worker-1/kafka
mkdir -p data/kind-worker-2/kafka
mkdir -p data/kind-worker-3/kafka

mkdir -p data/kind-worker-1/trino
mkdir -p data/kind-worker-2/trino
mkdir -p data/kind-worker-3/trino

mkdir -p data/kind-worker-1/flink
mkdir -p data/kind-worker-2/flink
mkdir -p data/kind-worker-3/flink

mkdir -p data/kind-worker-1/spark
mkdir -p data/kind-worker-2/spark
mkdir -p data/kind-worker-3/spark

mkdir -p data/kind-worker-1/starrocks
mkdir -p data/kind-worker-2/starrocks
mkdir -p data/kind-worker-3/starrocks

mkdir -p data/kind-worker-1/clickhouse
mkdir -p data/kind-worker-2/clickhouse
mkdir -p data/kind-worker-3/clickhouse

mkdir -p data/kind-worker-1/iceberg
mkdir -p data/kind-worker-2/iceberg
mkdir -p data/kind-worker-3/iceberg

mkdir -p data/kind-worker-1/common
mkdir -p data/kind-worker-2/common
mkdir -p data/kind-worker-3/common

#### create kind clusters
kind create cluster --config=kind-config.yaml --name=dataplatform
kubectl wait --for=condition=Ready nodes --all --timeout=120s


#### label the kind worker nodes
kubectl label node dataplatform-worker node-no=1
kubectl label node dataplatform-worker2 node-no=2
kubectl label node dataplatform-worker3 node-no=3


#### install cert manager
helm repo add jetstack https://charts.jetstack.io
helm repo update

kubectl create namespace cert-manager

helm install cert-manager jetstack/cert-manager \
  --namespace cert-manager \
  --version v1.15.0 \
  --set installCRDs=true
