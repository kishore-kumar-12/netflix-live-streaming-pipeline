from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, TimestampType

# ----------------------- SCHEMA -----------------------
schema = StructType([
    StructField("id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("type", StringType(), True),
    StructField("genre", StringType(), True),
    StructField("release_year", IntegerType(), True),
    StructField("country", StringType(), True),
    StructField("current_viewers", IntegerType(), True),
    StructField("total_views", IntegerType(), True),
    StructField("trending_score", DoubleType(), True),
    StructField("last_updated", StringType(), True)
])

# ----------------------- SPARK SESSION -----------------------
spark = (
    SparkSession.builder
    .appName("NetflixLiveConsumer")
    .config("spark.sql.shuffle.partitions", "1")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ----------------------- READ FROM KAFKA -----------------------
df_raw = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "netflix_live")
    .option("startingOffsets", "latest")
    .load()
)

# Kafka gives "value" as binary → convert to string
df_json = df_raw.selectExpr("CAST(value AS STRING)")

# ----------------------- PARSE JSON -----------------------
df_parsed = df_json.select(from_json(col("value"), schema).alias("data")).select("data.*")

# ----------------------- WRITE TO PARQUET -----------------------
output_path = "/home/kishore_kumar_/netflix_live_project/output_parquet"

query = (
    df_parsed.writeStream
    .format("parquet")
    .option("checkpointLocation", "/home/kishore_kumar_/netflix_live_project/checkpoints")
    .option("path", output_path)
    .outputMode("append")
    .start()
)

query.awaitTermination()
