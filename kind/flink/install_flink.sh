OPERATOR_VERSION="1.12.0"
HELM_REPO_URL="https://archive.apache.org/dist/flink/flink-kubernetes-operator-${OPERATOR_VERSION}/"
helm repo add flink-operator-repo $HELM_REPO_URL
helm repo update

# Create the flink-operator namespace if you haven't already
kubectl create namespace flink-operator --dry-run=client -o yaml | kubectl apply -f -

# Install the operator
helm install flink-kubernetes-operator flink-operator-repo/flink-kubernetes-operator -n flink-operator

kubectl create namespace flink

kubectl apply -f rbac.yaml


aws s3api create-bucket --bucket flink --region us-east-1 --endpoint-url http://localhost:9000 --profile minio
