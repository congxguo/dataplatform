helm repo add strimzi https://strimzi.io/charts/
helm repo update

kubectl create namespace kafka

helm install strimzi-kafka-operator strimzi/strimzi-kafka-operator \
  --namespace kafka \
  --set watchNamespaces="{kafka}"


kubectl apply -f storage.yaml
kubectl apply -f kafka-main.yaml
