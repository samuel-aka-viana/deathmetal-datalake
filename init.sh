curl -sfL https://get.k3s.io | sh -

kubectl apply -k cluster/
helm repo add nessie-helm https://charts.projectnessie.org
helm repo add trino https://trinodb.github.io/charts
helm install -n lakehouse nessie nessie-helm/nessie
helm install -n lakehouse -f charts/minio/values.yaml minio bitnami/minio
helm install -n lakehouse -f charts/trino/values.yaml trino trino/trino