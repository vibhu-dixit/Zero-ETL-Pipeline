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
print("Querying users table with new schema:")
spark.sql("SELECT id, name, email, signup_ts, age FROM mycatalog.db.users WHERE id IS NOT NULL ORDER BY signup_ts DESC, id DESC LIMIT 20").show(truncate=False)
spark.stop()