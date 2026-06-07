#### prepare storage dir for kind clusters
rm -rf data/kind-worker-1
rm -rf data/kind-worker-2
rm -rf data/kind-worker-3

mkdir -p data/kind-worker-1/minio
mkdir -p data/kind-worker-2/minio
mkdir -p data/kind-worker-3/minio

mkdir -p data/kind-worker-1/scylla
mkdir -p data/kind-worker-2/scylla
mkdir -p data/kind-worker-3/scylla

mkdir -p data/kind-worker-1/kafka
mkdir -p data/kind-worker-2/kafka
mkdir -p data/kind-worker-3/kafka

mkdir -p data/kind-worker-1/iceberg
mkdir -p data/kind-worker-2/iceberg
mkdir -p data/kind-worker-3/iceberg

mkdir -p data/kind-worker-1/nebula
mkdir -p data/kind-worker-2/nebula
mkdir -p data/kind-worker-3/nebula

mkdir -p data/kind-worker-1/milvus
mkdir -p data/kind-worker-2/milvus
mkdir -p data/kind-worker-3/milvus

mkdir -p data/kind-worker-1/es
mkdir -p data/kind-worker-2/es
mkdir -p data/kind-worker-3/es

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


#### install minio
kubectl create namespace minio
kubectl apply -f minio/storage.yaml
kubectl apply -f minio/headless.yaml
kubectl apply -f minio/sts.yaml


#### install flink-operator
helm repo add flink-kubernetes-operator https://downloads.apache.org/flink/flink-kubernetes-operator-1.10.0/
helm repo update
kubectl create namespace flink-operator
helm install flink-operator flink-kubernetes-operator/flink-kubernetes-operator -n flink-operator
#kubectl apply -f flink/rbac.yaml
