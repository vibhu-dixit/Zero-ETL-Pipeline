from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
load_dotenv()
spark = (
    SparkSession.builder
    .appName("IcebergShell")
    .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.mycatalog.type", "hadoop")
    .config("spark.sql.catalog.mycatalog.warehouse", os.getenv("AWS_BUCKET_NAME")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

spark.sql("SELECT * FROM mycatalog.db.users").show()