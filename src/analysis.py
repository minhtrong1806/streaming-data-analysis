from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sum as spark_sum, avg
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType


spark = SparkSession.builder \
    .appName("Taxi Trip Analysis") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()

schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("RatecodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("PULocationID", IntegerType(), True),
    StructField("DOLocationID", IntegerType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True),
    StructField("congestion_surcharge", DoubleType(), True),
    StructField("Airport_fee", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "trip_data") \
    .option("startingOffsets", "earliest") \
    .load()

# Giải mã dữ liệu từ định dạng JSON
trip_data = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")



# Thêm watermark dựa trên tpep_pickup_datetime
trip_data_with_watermark = trip_data.withWatermark("tpep_pickup_datetime", "15 minutes")

# Tính tổng số chuyến đi, tổng doanh thu, và khoảng cách trung bình
trip_analysis = trip_data_with_watermark.groupBy("VendorID").agg(
    spark_sum("total_amount").alias("Total_Revenue"),
    spark_sum("trip_distance").alias("Total_Distance"),
    avg("trip_distance").alias("Average_Distance"),
    spark_sum("passenger_count").alias("Total_Passengers")
)
# Ghi kết quả phân tích ra console
# Ghi kết quả phân tích ra console với outputMode 'complete'
query = trip_analysis.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()