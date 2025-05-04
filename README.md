# deathmetal-datalake

## Visão geral

Este projeto implementa Serverless Streaming Pipeline: sem gerenciamento de servidores para ingestão, usando serviços gerenciados (Kinesis, Firehose). para análise de dados de Death Metal, usando AWS simuladas pelo LocalStack, orquestração com Prefect e transformações de dados com Polars.

## Arquitetura

O pipeline de dados é dividido em três camadas principais, definidas por flows em Python e recursos AWS (simulados):

````text
csv/                            S3 Bucket (csv-batch-bucket)
  │                                ├─ landing/    (Raw via Firehose)
  │                                │
  │                                ├─ bronze/     (Parquet bruto)
  │                                │
  │                                └─ silver/     (Dados refinados)
  ▼
[landing.py]   (ingest_folder_flow)  ──> Kinesis Data Streams  ──> Firehose Delivery ──> S3 landing/ 
       │
       ▼
[bronze.py]    (landing→bronze-flow) ──> Converte CSV em Parquet (snappy) ──> S3 bronze/ 
       │
       ▼
[silver.py]    (silver-transform-flow) ──> Aplica tipagens, transforma e enriquece ──> S3 silver/ 

Infraestrutura local definida em Terraform (provider.tf, main.tf) e emulada via Docker Compose/LocalStack 
````
## Componentes

- **Terraform** (`provider.tf`, `main.tf`): define recursos AWS para S3, Kinesis Streams (albums-stream, bands-stream, reviews-stream), Firehose e dependências.
- **LocalStack** (`docker-compose.yml`): emula serviços AWS necessários (Kinesis, Firehose, S3, DynamoDB, IAM, STS).
- **Orquestração Prefect**:
  - `landing.py` — envia arquivos CSV do diretório `csv/` para os streams Kinesis.
  - `bronze.py` — lê objetos `landing/` em S3, normaliza e salva parquet bruto em `bronze/`.
  - `silver.py` — lê arquivo parquet bruto, aplica transformações de esquema e gera datasets finalizados em `silver/`.

## Pré-requisitos

- [Docker](https://www.docker.com/) e [Docker Compose](https://docs.docker.com/compose/)
- [Terraform](https://www.terraform.io/)
- Python 3.8+
- Instalar dependencias no requirements.txt

## Instalação

1. Clone este repositório:

   ```bash 
        git clone https://github.com/seu-usuario/deathmetal-datalake.git
   ```
cd deathmetal-datalake


2. Inicie o LocalStack:

   ```bash 
   docker-compose up -d

   ```


3. Provisione a infraestrutura:
   ```bash
        tflocal init
        tflocal plan
        tflocal apply 
   ```


4. Crie o diretório `csv/` e adicione os arquivos CSV de Death Metal:

   ```bash
      mkdir csv
   ```
   
````

## Estrutura de diretórios

```bash
deathmetal-datalake/
├── csv/                    # CSVs brutos (albums.csv, bands.csv, reviews.csv)
├── docker-compose.yml      # Configuração LocalStack 
├── infra/
    ├──main.tf                 # Infraestrutura Terraform
    ├── provider.tf             # Configuração do provider AWS para LocalStack
├──flows/
    ├── landing.py              # Flow de ingestão (Landing Zone) 
    ├── bronze.py               # Flow de conversão para Parquet (Bronze Zone)
    ├── silver.py               # Flow de refinamento (Silver Zone)
└── README.md               # Este arquivo
````

## Como executar

Execução sequencial dos flows Prefect:

```bash
# 1. Ingestão (Landing)
python flows/landing.py --folder csv

# 2. Processamento Bronze
python flows/bronze.py

# 3. Processamento Silver
python flows/silver.py
```

## Próximos passos

* Adicionar camada Gold com tabelas prontas para análise.
* Integrar monitoramento e alertas.
* Migrar para ambiente AWS real.

## Licença

Este projeto está sob a licença MIT.
