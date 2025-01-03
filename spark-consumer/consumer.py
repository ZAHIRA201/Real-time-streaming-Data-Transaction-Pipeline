from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, count, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType
from mysql_storage.insert_data import *

# Define the schema with nested structure
schema = StructType([
    StructField("payload", StructType([
        StructField("after", StructType([
            StructField("id", StringType(), True),
            StructField("brand", StringType(), True),
            StructField("screen_size", StringType(), True),
            StructField("ram", StringType(), True),
            StructField("rom", StringType(), True),
            StructField("sim_type", StringType(), True),
            StructField("battery", StringType(), True),
            StructField("price", StringType(), True)
        ]))
    ]))
])

checkpoint_location = 'C:\\Users\\DELL\\Desktop\\debeziu\\Real-Time-Streaming-Kafka-Debezium-Spark-Streaming\\checkpoint_spark'

# Initialize the Spark session
spark = SparkSession \
    .builder \
    .appName("Spark Kafka Real-Time Processing") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.4") \
    .getOrCreate()

# Set the log level to reduce verbosity
spark.sparkContext.setLogLevel("WARN")

# Read from Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "cdc.public.smartphones") \
    .option("startingOffsets", "earliest") \
    .load()

# Parse the Kafka value as JSON with nested structure
json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.payload.after.*")

# Cast the necessary columns to double
json_df = json_df \
    .withColumn("screen_size", col("screen_size").cast("double")) \
    .withColumn("ram", col("ram").cast("double")) \
    .withColumn("rom", col("rom").cast("double")) \
    .withColumn("battery", col("battery").cast("double")) \
    .withColumn("price", col("price").cast("double"))

json_df.printSchema()

# Perform the required aggregations and display all statistics in one DataFrame
statistics_df = json_df.agg(
    count("id").alias("total_phones"),
    spark_max("price").alias("max_price"),
    spark_max("screen_size").alias("max_screen_size"),
    spark_max("ram").alias("max_ram"),
    spark_max("rom").alias("max_rom"),
    spark_max("battery").alias("max_battery")
)


from pyspark.sql.types import DoubleType

statistics_df = statistics_df.select(
    col("total_phones").cast("int").alias("total_phones"),
    col("max_price").cast(DoubleType()).alias("max_price"),
    col("max_screen_size").cast(DoubleType()).alias("max_screen_size"),
    col("max_ram").cast(DoubleType()).alias("max_ram"),
    col("max_rom").cast(DoubleType()).alias("max_rom"),
    col("max_battery").cast(DoubleType()).alias("max_battery")
)

# Number of phones per brand
phones_per_brand_df = json_df.groupBy("brand").agg(
    count("id").alias("total_phones")
)

# Number of phones per sim type
phones_per_sim_type_df = json_df.groupBy("sim_type").agg(
    count("id").alias("total_phones")
)

# Max price per brand
max_price_per_brand_df = json_df.groupBy("brand").agg(
    spark_max("price").alias("max_price")
)

# Max price per sim type
max_price_per_sim_type_df = json_df.groupBy("sim_type").agg(
    spark_max("price").alias("max_price")
)

# Max RAM per brand
max_ram_per_brand_sim_df = json_df.groupBy("brand").agg(
    spark_max("ram").alias("max_ram")
)

# Max ROM per sim type
max_rom_per_brand_sim_df = json_df.groupBy("sim_type").agg(
    spark_max("rom").alias("max_rom")
)

# Max battery capacity per brand
max_battery_per_brand_sim_df = json_df.groupBy("brand").agg(
    spark_max("battery").alias("max_battery")
)

# Max screen size per sim type
max_screen_size_per_brand_sim_df = json_df.groupBy("sim_type").agg(
    spark_max("screen_size").alias("max_screen_size")
)

# Write streams to MySQL using foreachBatch
query_summary = statistics_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_statistics_summary) \
    .start()

query_phones_per_brand = phones_per_brand_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_phones_per_brand) \
    .start()

query_phones_per_sim_type = phones_per_sim_type_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_phones_per_sim_type) \
    .start()

query_max_price_per_brand = max_price_per_brand_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_price_per_brand) \
    .start()

query_max_price_per_sim_type = max_price_per_sim_type_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_price_per_sim_type) \
    .start()

query_max_ram_per_brand_sim = max_ram_per_brand_sim_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_ram_per_brand) \
    .start()

query_max_rom_per_brand_sim = max_rom_per_brand_sim_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_rom_per_sim_type) \
    .start()

query_max_battery_per_brand_sim = max_battery_per_brand_sim_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_battery_per_brand) \
    .start()

query_max_screen_size_per_brand_sim = max_screen_size_per_brand_sim_df \
    .writeStream \
    .outputMode("complete") \
    .foreachBatch(insert_data_mysql_max_screen_size_per_sim_type) \
    .start()

# Wait for all queries to finish
query_summary.awaitTermination()
query_phones_per_brand.awaitTermination()
query_phones_per_sim_type.awaitTermination()
query_max_price_per_brand.awaitTermination()
query_max_price_per_sim_type.awaitTermination()
query_max_ram_per_brand_sim.awaitTermination()
query_max_rom_per_brand_sim.awaitTermination()
query_max_battery_per_brand_sim.awaitTermination()
query_max_screen_size_per_brand_sim.awaitTermination()
