# Zero ETL Data Mesh – Project Overview

This project establishes a foundational streaming data pipeline, creating a **Zero-ETL Data Mesh** architecture. It leverages **Kafka** for high-throughput messaging, **Apache Spark Structured Streaming** for real-time data processing, and **Apache Iceberg** for robust, high-performance table format management on **AWS S3**. All components are orchestrated using **Docker Compose** for a streamlined local development environment.

## Project Goals & Progress (Weeks 1-3)

### **Week 1: Foundational Setup & Streaming Ingestion**

* **What we did:** Established a Dockerized environment for Kafka and Spark. Configured a Kafka topic (`users`) and built a Spark Structured Streaming job to ingest data from this topic. Enabled checkpointing for fault tolerance and exactly-once processing guarantees.

* **Why we did it:** To create a scalable and resilient core infrastructure for real-time data ingestion, capable of handling continuous data streams reliably.

### **Week 2: Apache Iceberg Integration & Basic Data Management**

* **What we did:** Integrated Apache Iceberg as the table format layer. Configured AWS S3 as the Iceberg warehouse, managing environment variables for secure access. Developed Spark jobs to initialize (`open_iceberg_shell.py`) and query (`query_users.py`) the Iceberg table. Demonstrated basic data management by performing `DELETE` operations on invalid rows.

* **Why we did it:** To introduce a powerful table format that enables schema evolution, time travel, and robust data management features directly on object storage, moving beyond basic file storage.

### **Week 3: Data Product Refinement – Schema Evolution, Partitioning & Deduplication**

* **What we did:**

    * **Schema Evolution:** Evolved the `users` table schema by adding new fields (`signup_ts`, `age`) to accommodate richer user data.

    * **Partitioning:** Implemented partitioning by `days(signup_ts)` and `bucket(16, id)` to physically organize data on S3.

    * **Deduplication:** Developed a robust streaming deduplication strategy to ensure uniqueness of records based on `id`, always reflecting the latest data.

* **Why we did it:**

    * **Schema Evolution:** To maintain agility as data requirements change, allowing schema updates without downtime or complex data migrations.

    * **Partitioning:** To significantly boost query performance for analytical workloads by enabling data pruning at the storage layer.

    * **Deduplication:** To ensure high data quality and consistency in a streaming environment, preventing duplicate records from polluting analytical datasets.

**Key Challenges & Solutions during Week 3:**
This week involved navigating several complex distributed computing challenges:

* **Data Type Mismatches & Nulls:** Initial issues arose from `id` being consumed as `null` or type conflicts (e.g., `STRING` vs. `INT`).

    * **Resolution:** Ensured meticulous type alignment across Kafka producer, Spark consumer schema, and Iceberg table definitions.

* **`MERGE INTO` Operation Failures:** Encountered `TABLE_OR_VIEW_NOT_FOUND` errors with temporary views and `MERGE INTO is not supported` exceptions.

    * **Resolution:** Addressed view scoping within `foreachBatch` using a dedicated `SparkSession` instance. Crucially, discovered and applied `TBLPROPERTIES ('format-version'='2')` when creating the Iceberg table to enable necessary row-level operations for `MERGE INTO`.

* **Persistent `MERGE INTO` Instability:** Despite enabling format version 2, `MERGE INTO` proved unreliable in the specific environment.

    * **Resolution:** Pivoted to a highly robust **overwrite-based deduplication strategy**. Each micro-batch now reads the current table, unions it with new data, performs in-memory deduplication (keeping the latest record by `signup_ts` for each `id`), and then atomically overwrites the entire Iceberg table. This guarantees a clean, up-to-date snapshot with every interval.

## Technologies Used

* Docker Compose

* Apache Spark 3.4+

* Apache Kafka

* Apache Iceberg

* Python 3

* S3-compatible storage (AWS S3)

## Project File Structure (Current)
```
.
├── jobs/
│   ├── kafka_producer.py      # Produces sample user data to Kafka
│   ├── kafka_to_iceberg.py    # Spark Structured Streaming consumer (main pipeline)
│   ├── open_iceberg_shell.py  # Initializes/recreates Iceberg table schema
│   └── query_users.py         # Queries the Iceberg user table
├── configs/
│   └── spark-defaults.conf    # (Optional) Spark configuration defaults
├── docker-compose.yaml
├── Dockerfile
├── .env                       # Environment variables for AWS credentials & S3 bucket
├── .gitignore
└── README.md                  # Project documentation
```
## How to Run

1.  **Clone this repo and navigate into the project directory:**

    ```
    git clone <your-repo-url>
    cd zero-etl-data-mesh

    ```

2.  **Create a `.env` file** in the project root with your AWS credentials and S3 bucket name:

    ```
    AWS_ACCESS_KEY_ID=your-access-key
    AWS_SECRET_ACCESS_KEY=your-secret-key
    AWS_BUCKET_NAME=your_unique_s3_bucket_name

    ```

3.  **Start Docker containers:**

    ```
    docker compose up -d --build

    ```

    Ensure Zookeeper and Kafka services are healthy (`docker compose ps`).

4.  **Perform Initial S3 Cleanup (Crucial for a clean state):**

    * Delete all existing Iceberg table data:

        ```
        aws s3 rm --recursive s3://your_unique_s3_bucket_name/warehouse/db/users/

        ```

    * Delete all Spark Streaming checkpoints and temporary data:

        ```
        aws s3 rm --recursive s3://your_unique_s3_bucket_name/checkpoints/
        aws s3 rm --recursive s3://your_unique_s3_bucket_name/temp_data/

        ```

5.  **Exec into the Spark container:**

    ```
    docker exec -it zero-etl-data-mesh-spark-1 bash

    ```

    (All subsequent `spark-submit` and `python` commands are run from *inside* this container's bash session, or by replacing `bash` with the command directly in `docker exec` from your host.)

6.  **Initialize/Recreate the Iceberg Table:**

    ```
    spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.1 \
      /app/jobs/open_iceberg_shell.py

    ```

7.  **Start the Kafka Producer (in a separate terminal):**

    ```
    docker exec zero-etl-data-mesh-spark-1 python /app/jobs/kafka_producer.py

    ```

    Let it run for 30-60 seconds to pre-populate Kafka with messages.

8.  **Start the Spark Structured Streaming Consumer (in another separate terminal):**

    ```
    docker exec zero-etl-data-mesh-spark-1 spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.1,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1 \
      /app/jobs/kafka_to_iceberg.py

    ```

    Keep this running to continuously process data.

9.  **Query the Iceberg Table (in yet another separate terminal):**

    ```
    docker exec zero-etl-data-mesh-spark-1 spark-submit \
      --packages org.apache.iceberg:iceberg-spark-runtime-3.4_2.12:1.4.3,org.apache.hadoop:hadoop-aws:3.3.1 \
      /app/jobs/query_users.py

    ```

    Observe the deduplicated data with all fields populated. To test deduplication, stop and restart the `kafka_producer.py` to generate duplicate IDs, then re-run this query to see updates.
