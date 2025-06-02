import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StringType, IntegerType
from dotenv import load_dotenv
load_dotenv()
schema = StructType() \
    .add("id", IntegerType()) \
    .add("name", StringType()) \
    .add("email", StringType())

spark = SparkSession.builder \
    .appName("KafkaToIceberg") \
    .config("spark.sql.catalog.mycatalog", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.mycatalog.type", "hadoop") \
    .config("spark.sql.catalog.mycatalog.warehouse", "s3a://zero-etl-mesh-demo/warehouse") \
    .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID")) \
    .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .getOrCreate()

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "users") \
    .load()

parsed_df = raw_df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query =parsed_df.writeStream \
    .format("iceberg") \
    .outputMode("append") \
    .option("checkpointLocation", "s3a://zero-etl-mesh-demo/checkpoints/users") \
    .toTable("mycatalog.db.users")
query.awaitTermination()
