kubectl create namespace client
kubectl apply -f python_client.yaml
kubectl wait --for=condition=Ready pod/python-toolbox -n client --timeout=180s
kubectl cp prepare-env.sh client/python-toolbox:/opt/prepare-env.sh
kubectl exec -it python-toolbox -n client -- sh /opt/prepare-env.sh
