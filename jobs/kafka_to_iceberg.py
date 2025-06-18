import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, to_timestamp, row_number, desc
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType
from pyspark.sql.window import Window # Import Window for deduplication

from dotenv import load_dotenv

load_dotenv()
# Define the schema for the incoming Kafka JSON messages
json_schema = StructType() \
    .add("id", StringType()) \
    .add("name", StringType()) \
    .add("email", StringType()) \
    .add("signup_ts", StringType()) \
    .add("age", IntegerType())

# Initialize Spark Session with Iceberg extensions and Hadoop catalog
spark = SparkSession.builder \
    .appName("KafkaToIcebergDeduplicator") \
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

# Read from Kafka stream
raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "users") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the JSON data and select/cast columns
parsed_df_intermediate = raw_df.select(from_json(col("value").cast(StringType()), json_schema).alias("data"))

# Select individual fields from the 'data' struct and convert signup_ts to TimestampType
processed_df = parsed_df_intermediate.select(
    col("data.id").alias("id"),
    col("data.name").alias("name"),
    col("data.email").alias("email"),
    to_timestamp(col("data.signup_ts"), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").alias("signup_ts"),
    col("data.age").alias("age")
)

# --- FOREACH BATCH WRITE FOR DEDUPLICATION (OVERWRITE STRATEGY) ---
def upsert_to_iceberg(batch_df, batch_id):
    # Get the current SparkSession for the executor to ensure correct scope
    current_spark_session = SparkSession.builder.getOrCreate()

    if batch_df.isEmpty():
        print(f"--- Batch {batch_id} is empty, skipping deduplication ---")
        return

    print(f"--- Processing Batch {batch_id} for deduplication via overwrite ---")

    try:
        # Read the current state of the Iceberg table
        existing_table_df = current_spark_session.table("mycatalog.db.users")
        # Combine the existing data with the new batch data
        combined_df = existing_table_df.unionByName(batch_df, allowMissingColumns=True)
        # Deduplicate: Keep the record with the latest signup_ts for each unique ID
        window_spec = Window.partitionBy("id").orderBy(desc("signup_ts"))
        deduplicated_df = combined_df.withColumn("row_num", row_number().over(window_spec))\
                                        .filter(col("row_num") == 1)\
                                        .drop("row_num")

        deduplicated_df.writeTo("mycatalog.db.users") \
            .using("iceberg") \
            .overwritePartitions()

        print(f"--- Batch {batch_id} deduplication via overwrite completed successfully ---")

    except Exception as e:
        print(f"--- Error during deduplication for Batch {batch_id}: {e} ---")
        raise

query = processed_df.writeStream \
    .foreachBatch(upsert_to_iceberg) \
    .option("checkpointLocation", "s3a://zero-etl-mesh-demo/checkpoints/users_deduplication_checkpoint") \
    .trigger(processingTime="10 seconds") \
    .start()
print("Spark Structured Streaming job with deduplication (overwrite strategy) started. Waiting for termination...")
query.awaitTermination()
