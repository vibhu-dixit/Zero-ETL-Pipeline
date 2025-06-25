from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
load_dotenv()
spark = SparkSession.builder \
    .appName("QueryUsers") \
    .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.mycatalog.type", "hadoop") \
    .config("spark.sql.catalog.mycatalog.warehouse", os.getenv("AWS_BUCKET_NAME")) \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.sql.default.catalog", "mycatalog") \
    .getOrCreate()
print("--- Time travel to a specific snapshot using VERSION AS OF ---")
spark.sql("SELECT * FROM mycatalog.db.users VERSION AS OF 27261393872153004").show(truncate=False)
spark.stop()