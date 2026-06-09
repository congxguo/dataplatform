docker exec dataplatform-worker sysctl -w fs.aio-max-nr=1048576
docker exec dataplatform-worker2 sysctl -w fs.aio-max-nr=1048576
docker exec dataplatform-worker3 sysctl -w fs.aio-max-nr=1048576

kubectl create namespace scylla
kubectl apply -f scylla-sa.yaml
kubectl apply -f scylla-storage.yaml
kubectl apply -f scylla-configmap.yaml
kubectl apply -f scylla.yaml
