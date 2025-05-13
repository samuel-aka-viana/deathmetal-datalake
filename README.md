## Arquitetura

![diagrama](docs/architecture.png)

| Camada | Tecnologia | Descrição |
|--------|------------|-----------|
| Storage | **MinIO** | Bucket `datalake` (S3-compatible) |
| Catálogo | **Project Nessie** | Versiona tabelas Iceberg (`main`, `dev`, tags) |
| Formato | **Apache Iceberg v2** | Snapshots/Schemas evolutivos |
| Compute | **Trino 443+** | SQL interativo (`nessie` catalog) |
| ETL | **Daft** | Ingestão + transformações paralelas |
| Orquestração | **Prefect 2** | Deployments → Work Pool |

python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt -e .
prefect server start            # UI em localhost:4200
prefect work-pool create -t process local-process
prefect deploy                  # gera e aplica todos os deployments
prefect agent start -p local-process

prefect deployment run 'landing-flow/local-process'


# instalar k3s
curl -sfL https://get.k3s.io | sh -      # cria kubeconfig
# namespace + secrets (Terraform opcional)
kubectl apply -f cluster/k8s/namespace.yaml

[//]: # (terraform -chdir=infra/terraform apply   # gera Secret minio-creds)
# Helm charts
helm install -n lakehouse minio  bitnami/minio   -f cluster/helm/minio.yaml
helm install -n lakehouse nessie nessie-helm/nessie
helm install -n lakehouse trino  trino/trino     -f cluster/helm/trino.yaml
# Prefect
helm install -n lakehouse prefect prefecthq/prefect

prefect work-pool create -t kubernetes lakehouse-k8s \
  --namespace lakehouse \
  --image ghcr.io/samuel-aka-viana/dmlakehouse:latest

kubectl apply -n lakehouse -f cluster/prefect-worker.yaml


prefect deploy

prefect deployment run "landing-flow/lakehouse-k8s"
prefect deployment run "bronze-flow/lakehouse-k8s"
prefect deployment run "silver-flow/lakehouse-k8s"
prefect deployment run "gold-flow/lakehouse-k8s"
