import os
from pyspark.sql import SparkSession
from dotenv import load_dotenv

load_dotenv()
spark = (
    SparkSession.builder
    .appName("IcebergTableRecreation")
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

print("Creating new users table with schema (ID as STRING) and partitioning...")
spark.sql("""
    CREATE OR REPLACE TABLE mycatalog.db.users (
        id STRING,    
        name STRING,
        email STRING,
        signup_ts TIMESTAMP,
        age INT
    )
    USING iceberg
    PARTITIONED BY (days(signup_ts), bucket(16, id))
    LOCATION 's3a://zero-etl-mesh-demo/warehouse/db/users'
    TBLPROPERTIES ('format-version'='2')
""")
print("New partitioned table 'mycatalog.db.users' created successfully.")

print("\nNew table schema:")
spark.sql("DESCRIBE TABLE mycatalog.db.users;").show()

spark.stop()
