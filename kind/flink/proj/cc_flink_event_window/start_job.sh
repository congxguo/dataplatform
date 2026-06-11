kubectl apply -f flink-job.yaml

sleep 20

kubectl port-forward svc/event-window-job-rest 8081:8081 -n flink
