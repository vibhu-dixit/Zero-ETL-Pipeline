from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os

load_dotenv()

def compact_iceberg_table():
    spark = SparkSession.builder \
        .appName("IcebergTableCompaction") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.mycatalog.type", "hadoop") \
        .config("spark.sql.catalog.mycatalog.warehouse", os.getenv("AWS_BUCKET_NAME")) \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.path.style.access", "true") \
        .config("spark.sql.default.catalog", "mycatalog") \
        .getOrCreate()

    try:
        print("Starting Iceberg table compaction for mycatalog.db.users...")
        spark.sql("CALL mycatalog.system.rewrite_data_files('db.users')")
        print("Iceberg table compaction completed.")

    except Exception as e:
        print(f"Error during compaction: {e}")
    finally:
        spark.stop()

if __name__ == "__main__":
    compact_iceberg_table()