# Zero ETL Data Mesh – Week 1 Setup

This project sets up a basic streaming pipeline using **Kafka**, **Apache Spark**, and **Apache Iceberg**, writing data to an S3 bucket with no intermediate ETL layer.

---

## Week 1 Goals Completed
- ✅ Dockerized Kafka + Spark setup
- ✅ Kafka topic `users` configured
- ✅ Spark Structured Streaming reads from Kafka
- ✅ Iceberg table written to S3
- ✅ Environment config via `.env` file
- ✅ Checkpointing enabled for exactly-once guarantees

---

## Technologies
- Docker Compose
- Apache Spark 3.4+
- Apache Kafka
- Apache Iceberg
- Python 3
- S3-compatible storage (AWS or MinIO)

---
## Create a `.env` file:

- AWS_ACCESS_KEY_ID=your-access-key
- AWS_SECRET_ACCESS_KEY=your-secret-key
- AWS_BUCKET_NAME=Your_Bucket_Name
## Start containers:

`docker-compose up -d`

## Exec into Spark container:

`docker exec -it zero-etl-data-mesh-spark-1 bash`

## Run the pipeline:

`spark-submit \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3 \
  jobs/kafka_to_iceberg.py`

## Project File Structure till now
`
├── jobs/
│   └── kafka_to_iceberg.py  # Main pipeline script
├── configs/
│   └── spark-defaults.conf  # (Optional) Spark config
├── docker-compose.yaml
├── Dockerfile
├── .env
├── .gitignore
└── README.md
`
## How to Run

1. Clone this repo and navigate into the project:
   ```bash
   git clone <your-repo-url>
   cd zero-etl-data-mesh
