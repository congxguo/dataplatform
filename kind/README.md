# 🏗️ Data Platform on kind

A local Kubernetes-based data platform built with [kind](https://kind.sigs.k8s.io/) (Kubernetes IN Docker), designed for data engineering testing, verification, and learning — all running on your laptop.

---

## What is kind?

[kind](https://kind.sigs.k8s.io/) (**K**ubernetes **IN** **D**ocker) is a tool for running local Kubernetes clusters using Docker containers as nodes. It was originally designed for testing Kubernetes itself, but it's an excellent choice for:

- **Local development** — spin up a realistic Kubernetes environment without a cloud provider
- **CI/CD pipelines** — lightweight clusters that start in seconds
- **Learning & experimentation** — explore Kubernetes and cloud-native tooling risk-free

kind requires only Docker and `kubectl` to get started, making it one of the most accessible ways to run a full Kubernetes cluster locally.

```bash
# Install kind
brew install kind          # macOS
choco install kind         # Windows
go install sigs.k8s.io/kind@latest  # via Go

# Create a cluster
kind create cluster --name dataplatform

# Verify
kubectl cluster-info --context kind-dataplatform
```

---

## About This Project

This repository provisions a **local data platform** on top of a kind cluster, mirroring the core components you'd find in a production data infrastructure — without the cost or complexity of a cloud environment.

### 🎯 Purpose

| Goal | Description |
|------|-------------|
| **Testing** | Validate data pipeline logic, ingestion jobs, and transformations in an isolated environment |
| **Verification** | Confirm that platform configurations, Helm charts, and manifests behave as expected before deploying to staging or production |
| **Learning** | Explore data platform tooling (Spark, Trino, Airflow, Kafka, etc.) hands-on without needing cloud credentials or incurring costs |

### 🧩 What's Included

| Component | Description |
|-----------|-------------|
| **Apache Flink** | Stream & batch processing via the Flink operator on Kubernetes, with S3-compatible storage support and an Iceberg integration example |
| **Apache Iceberg** | REST catalog deployment for table-format management, enabling ACID transactions and schema evolution |
| **Apache Kafka** | Event streaming layer deployed with KRaft mode (no ZooKeeper), including node pool and persistent storage configuration |

### 🗂️ Repository Structure

```
kind/
├── bootstrap.sh              # One-shot script to bring up the full platform
│
├── flink/
│   ├── flink-s3/             # Custom Flink image with S3 (Hadoop) filesystem support
│   │   ├── Dockerfile
│   │   ├── build_image.sh
│   │   └── flink-s3-fs-hadoop-1.20.0.jar
│   ├── flink-session.yaml    # Flink session cluster manifest
│   ├── install_flink.sh      # Flink operator installation script
│   ├── rbac.yaml             # RBAC rules for the Flink operator
│   ├── run_sql_client.sh     # Helper to launch the Flink SQL client
│   └── proj/
│       ├── cc_flink_app/     # Basic Flink DataStream job (Java/Maven)
│       └── cc_flink_iceberg/ # Flink job writing to an Iceberg table via the REST catalog
│
├── iceberg/
│   ├── deploy.sh             # Deploy the Iceberg REST catalog
│   └── iceberg-rest-catalog.yaml
│
└── kafka/
    ├── deploy.sh             # Deploy Kafka to the cluster
    ├── kafka-main.yaml       # Main Kafka cluster manifest
    ├── nodepool.yaml         # Kafka node pool configuration
    └── storage.yaml          # Persistent storage for Kafka
```

---

## Getting Started

### Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (≥ 20.10)
- [kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation) (≥ 0.20)
- [kubectl](https://kubernetes.io/docs/tasks/tools/)
- [Helm](https://helm.sh/docs/intro/install/) (≥ 3.x)

### Bootstrap the Platform

```bash
# Clone the repo
git clone https://github.com:congxguo/dataplatform.git
cd dataplatform/kind

# Create the kind cluster
kind create cluster --name dataplatform

# Deploy all platform components
cd kind && ./bootstrap.sh
```

### Tear Down

```bash
kind delete cluster --name dataplatform
```

---

## Use Cases

- **Flink job development** — build and test DataStream or Table API jobs locally before deploying to a managed service
- **Iceberg table workflows** — experiment with schema evolution, time travel, and partition management via the REST catalog
- **Kafka pipeline testing** — produce and consume events end-to-end in an isolated cluster
- **Flink + Iceberg integration** — validate streaming writes into Iceberg tables with exactly-once semantics
- **Onboarding new engineers** to the data stack without cloud credentials or costs
- **Reproducing production issues** locally in a fully controlled environment

---

## Contributing

Contributions are welcome! Please open an issue or pull request for new components, bug fixes, or documentation improvements.

---

## License

[MIT](LICENSE)
