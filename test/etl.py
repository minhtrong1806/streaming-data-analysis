import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from influxdb_client import InfluxDBClient, Point, WritePrecision

# Cấu hình logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Tạo SparkSession
spark = SparkSession.builder \
    .appName("Taxi Trip ETL") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.2") \
    .getOrCreate()

# Định nghĩa schema cho dữ liệu
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


trip_data = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

influx_client = InfluxDBClient(
    url="http://localhost:8086",
    token="admin",
    org="org"
)

write_api = influx_client.write_api()

def write_to_influx(batch_df, batch_id):
    points = []
    
    # Chuyển đổi từng hàng thành điểm InfluxDB
    rows = batch_df.collect()  # Lấy tất cả hàng trong batch
    for row in rows:
        try:
            # Tạo Point cho InfluxDB
            point = Point("taxi_trip") \
                .tag("VendorID", row.VendorID) \
                .tag("PULocationID", row.PULocationID) \
                .tag("DOLocationID", row.DOLocationID) \
                .field("passenger_count", row.passenger_count) \
                .field("trip_distance", row.trip_distance) \
                .field("fare_amount", row.fare_amount) \
                .field("total_amount", row.total_amount) \
                .field("tip_amount", row.tip_amount) \
                .time(row.tpep_pickup_datetime, WritePrecision.NS)
            
            points.append(point)
        except Exception as e:
            logger.error(f"Error processing row {row}: {e}")
    
    # Ghi một lần cho tất cả điểm
    if points:
        try:
            write_api.write(bucket="tlc", record=points)
            logger.info(f"Wrote {len(points)} points to InfluxDB")
        except Exception as e:
            logger.error(f"Failed to write points to InfluxDB: {e}")

# Ghi dữ liệu vào InfluxDB
query = trip_data.writeStream \
    .foreachBatch(write_to_influx) \
    .outputMode("append") \
    .start()

logger.info("Starting streaming...")

# Chờ cho đến khi kết thúc
query.awaitTermination()
logger.info("Streaming stopped.")
