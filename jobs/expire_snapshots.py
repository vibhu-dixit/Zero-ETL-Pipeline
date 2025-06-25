from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Initialize Spark Session with Iceberg extensions and Hadoop catalog
spark = (
    SparkSession.builder
    .appName("IcebergSnapshotExpiration")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.mycatalog.type", "hadoop")
    .config("spark.sql.catalog.mycatalog.warehouse", os.getenv("AWS_BUCKET_NAME"))
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.sql.default.catalog", "mycatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# Expire snapshots older than 1 hour for testing
# You can adjust this timedelta to your desired retention period.
expiration_timestamp = (datetime.now() - timedelta(seconds=30)).isoformat(timespec='milliseconds') + 'Z'

print(f"Expiring snapshots in mycatalog.db.users older than {expiration_timestamp}...")
spark.sql(f"CALL mycatalog.system.expire_snapshots(table => 'db.users', older_than => TIMESTAMP '{expiration_timestamp}')").show()

# To verify, you can query the history again and check that older snapshots have been removed
print("--- Verifying history after expiration ---")
spark.sql("SELECT * FROM mycatalog.db.users.history ORDER BY made_current_at DESC").show(truncate=False)

spark.stop()