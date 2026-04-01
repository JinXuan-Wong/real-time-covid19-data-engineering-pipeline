# Author: Wong Jin Xuan
# Task 1
"""Kafka -> HDFS raw landing for Task 1 (Parquet, partitioned)"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, date_format, from_unixtime
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType

SCHEMA = StructType([
    StructField("country", StringType()),
    StructField("continent", StringType()),
    StructField("cases", LongType()),
    StructField("todayCases", LongType()),
    StructField("deaths", LongType()),
    StructField("todayDeaths", LongType()),
    StructField("recovered", LongType()),
    StructField("active", LongType()),
    StructField("critical", LongType()),
    StructField("tests", LongType()),
    StructField("population", LongType()),
    StructField("casesPerOneMillion", DoubleType()),
    StructField("deathsPerOneMillion", DoubleType()),
    StructField("updated", LongType())
])

class KafkaToHDFSRaw:
    def __init__(self, bootstrap_servers, topic, sink_path, checkpoint_path):
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic
        self.sink_path = sink_path
        self.checkpoint_path = checkpoint_path

    def start(self):
        spark = (SparkSession.builder
                 .appName("covid19_task1_kafka_to_hdfs_raw")
                 .getOrCreate())
        spark.sparkContext.setLogLevel("WARN")

        df = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", self.bootstrap_servers)
              .option("subscribe", "self.topic1.self.topic2")
              .option("startingOffsets", "latest")
              .load()
              .selectExpr("CAST(value AS STRING) AS json"))

        parsed = (
            df.select(from_json(col("json"), SCHEMA).alias("d"))
              .where(col("d").isNotNull())
              .select("d.*")
              .withColumn("event_ts", to_timestamp(from_unixtime((col("updated")/1000))))
              .withColumn("event_date", date_format(col("event_ts"), "yyyy-MM-dd"))
        )

        (parsed.writeStream
            .format("parquet")
            .option("path", self.sink_path)
            .option("checkpointLocation", self.checkpoint_path)
            .partitionBy("event_date")
            .outputMode("append")
            .start()
            .awaitTermination())

if __name__ == "__main__":
    KafkaToHDFSRaw(
        bootstrap_servers="localhost:9092",
        topic="covid19_stream",
        sink_path="hdfs://localhost:9000/user/student/covid19/raw",
        checkpoint_path="hdfs://localhost:9000/user/student/checkpoints/covid19_raw",
    ).start()
