helm repo add trino https://trinodb.github.io/charts

helm repo update

kubectl create namespace trino

helm install trino trino/trino \
  -n trino \
  --create-namespace \
  -f values.yaml

kubectl port-forward svc/trino 8080:8080 -n trino
