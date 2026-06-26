from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, max, min
from pyspark.sql.types import StructType, StringType, DoubleType, IntegerType
from influxdb import InfluxDBClient

spark = SparkSession.builder \
    .appName("WeatherSparkToInfluxDB") \
    .getOrCreate()

spark.sparkContext.setLogLevel("ERROR")

schema = StructType() \
    .add("city", StringType()) \
    .add("temp_c", DoubleType()) \
    .add("humidity", IntegerType()) \
    .add("wind_kph", DoubleType()) \
    .add("condition", StringType()) \
    .add("time", StringType())

raw_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "weather_topic") \
    .option("startingOffsets", "latest") \
    .load()

weather_df = raw_df.selectExpr("CAST(value AS STRING) as json_data") \
    .select(from_json(col("json_data"), schema).alias("data")) \
    .select("data.*") \
    .dropna()

analysis_df = weather_df.groupBy("city").agg(
    avg("temp_c").alias("avg_temp"),
    max("temp_c").alias("max_temp"),
    min("temp_c").alias("min_temp"),
    avg("humidity").alias("avg_humidity"),
    avg("wind_kph").alias("avg_wind")
)

def write_to_influx(batch_df, batch_id):
    rows = batch_df.collect()

    if not rows:
        return

    client = InfluxDBClient(
        host="localhost",
        port=8086,
        database="weather_db"
    )

    points = []

    for row in rows:
        points.append({
            "measurement": "weather_analysis",
            "tags": {
                "city": row["city"]
            },
            "fields": {
                "avg_temp": float(row["avg_temp"]),
                "max_temp": float(row["max_temp"]),
                "min_temp": float(row["min_temp"]),
                "avg_humidity": float(row["avg_humidity"]),
                "avg_wind": float(row["avg_wind"])
            }
        })

    client.write_points(points)
    client.close()

    print("Batch", batch_id, "written to InfluxDB")

query = analysis_df.writeStream \
    .outputMode("complete") \
    .foreachBatch(write_to_influx) \
    .trigger(processingTime="10 seconds") \
    .start()

query.awaitTermination()
