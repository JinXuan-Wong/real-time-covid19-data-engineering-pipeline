# Author: Wong Jin Xuan (Task 5)

from __future__ import annotations
import argparse, os
from pyspark.sql import SparkSession, functions as F, types as T, DataFrame

SCHEMA = T.StructType([
    T.StructField("country", T.StringType()),
    T.StructField("continent", T.StringType()),
    T.StructField("cases", T.LongType()),
    T.StructField("todayCases", T.LongType()),
    T.StructField("deaths", T.LongType()),
    T.StructField("todayDeaths", T.LongType()),
    T.StructField("recovered", T.LongType()),
    T.StructField("active", T.LongType()),
    T.StructField("critical", T.LongType()),
    T.StructField("tests", T.LongType()),
    T.StructField("population", T.LongType()),
    T.StructField("casesPerOneMillion", T.DoubleType()),
    T.StructField("deathsPerOneMillion", T.DoubleType()),
    T.StructField("updated", T.LongType()),
    T.StructField("countryInfo", T.StructType([
        T.StructField("_id", T.IntegerType()),
        T.StructField("iso2", T.StringType()),
        T.StructField("iso3", T.StringType()),
        T.StructField("lat", T.DoubleType()),
        T.StructField("long", T.DoubleType()),
        T.StructField("flag", T.StringType())
    ]))
])

class Task5StreamingPipeline:
    def __init__(self, args: argparse.Namespace):
        self.args = args
        self.spark = (SparkSession.builder
                      .appName("covid19_task5_streaming_fresh")
                      .config("spark.sql.shuffle.partitions", "2")
                      .getOrCreate())
        self.spark.sparkContext.setLogLevel("INFO")

    def read_kafka_stream(self) -> DataFrame:
        s = (self.spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", self.args.kafka)
              .option("subscribe", self.args.in_topic)
              .option("startingOffsets", self.args.startingOffsets)
              .option("failOnDataLoss", "false")
              .load()
              .selectExpr("CAST(value AS STRING) AS json"))
        return s

    def clean_enrich(self, df_json: DataFrame) -> DataFrame:
        d = df_json.select(F.from_json(F.col("json"), SCHEMA).alias("d")).select("d.*")
        event_ts = F.to_timestamp(F.from_unixtime(F.col("updated")/1000.0))
        d = (d.withColumn("country", F.initcap(F.trim(F.col("country"))))
               .withColumn("population", F.col("population").cast("long"))
               .withColumn("cases", F.col("cases").cast("long"))
               .withColumn("deaths", F.col("deaths").cast("long"))
               .withColumn("tests", F.col("tests").cast("long"))
               .withColumn("event_ts", event_ts)
               .withColumn("event_date", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))
               .withColumn("region",
                   F.when(F.col("continent")=="Asia","APAC")
                    .when(F.col("continent")=="Europe","EMEA")
                    .when(F.col("continent")=="Africa","MEA")
                    .when(F.col("continent")=="North America","NA")
                    .when(F.col("continent")=="South America","SA")
                    .when(F.col("continent")=="Australia-Oceania","Oceania")
                    .otherwise("Other"))
               .withColumn("latitude",  F.col("countryInfo.lat"))
               .withColumn("longitude", F.col("countryInfo.long"))
               .withColumn("flag_url",  F.col("countryInfo.flag"))
               .drop("countryInfo"))
        d = (d.filter(F.col("country").isNotNull() & F.col("population").isNotNull() & (F.col("population") > 0))
               .filter(F.col("cases") >= 0)
               .withWatermark("event_ts", "30 seconds")
               .dropDuplicates(["country", "updated"]))
        d = (d.withColumn("cases_per_million",  F.col("cases") / F.col("population") * F.lit(1_000_000.0))
               .withColumn("deaths_per_million", F.col("deaths") / F.col("population") * F.lit(1_000_000.0))
               .withColumn("tests_per_thousand", F.col("tests")  / F.col("population") * F.lit(1_000.0)))
        return d

    def clean_enrich_for_dashboard(self, df_json: DataFrame) -> DataFrame:
        d = df_json.select(F.from_json(F.col("json"), SCHEMA).alias("d")).select("d.*")
        event_ts = F.to_timestamp(F.from_unixtime(F.col("updated")/1000.0))
        d = (d.withColumn("country", F.initcap(F.trim(F.col("country"))))
               .withColumn("population", F.col("population").cast("long"))
               .withColumn("cases", F.col("cases").cast("long"))
               .withColumn("deaths", F.col("deaths").cast("long"))
               .withColumn("tests", F.col("tests").cast("long"))
               .withColumn("event_ts", event_ts)
               .withColumn("event_date", F.date_format(F.col("event_ts"), "yyyy-MM-dd"))
               .withColumn("region",
                   F.when(F.col("continent")=="Asia","APAC")
                    .when(F.col("continent")=="Europe","EMEA")
                    .when(F.col("continent")=="Africa","MEA")
                    .when(F.col("continent")=="North America","NA")
                    .when(F.col("continent")=="South America","SA")
                    .when(F.col("continent")=="Australia-Oceania","Oceania")
                    .otherwise("Other"))
               .withColumn("latitude",  F.col("countryInfo.lat"))
               .withColumn("longitude", F.col("countryInfo.long"))
               .withColumn("flag_url",  F.col("countryInfo.flag"))
               .drop("countryInfo"))
        d = (d.filter(F.col("country").isNotNull() & F.col("population").isNotNull() & (F.col("population") > 0))
               .filter(F.col("cases") >= 0))
        d = (d.withColumn("cases_per_million",  F.col("cases") / F.col("population") * F.lit(1_000_000.0))
               .withColumn("deaths_per_million", F.col("deaths") / F.col("population") * F.lit(1_000_000.0))
               .withColumn("tests_per_thousand", F.col("tests")  / F.col("population") * F.lit(1_000.0)))
        return d

    def window_region_agg(self, d: DataFrame) -> DataFrame:
        w = F.window(F.col("event_ts"), "30 seconds")
        agg = (d.groupBy(w.alias("w"), F.col("region"))
                 .agg(F.sum("cases").alias("sum_cases"),
                      F.sum("deaths").alias("sum_deaths"),
                      F.sum("tests").alias("sum_tests"))
                 .withColumn("death_rate_pct",
                             F.when(F.col("sum_cases") > 0, F.col("sum_deaths")*100.0/F.col("sum_cases")).otherwise(0.0))
                 .select(F.col("region"),
                         F.col("w.start").alias("window_start"),
                         F.col("w.end").alias("window_end"),
                         "sum_cases","sum_deaths","sum_tests","death_rate_pct"))
        return agg

    @staticmethod
    def _hdfs_path_exists(spark: SparkSession, path: str) -> bool:
        try:
            jvm = spark._jvm
            sc = spark.sparkContext
            uri = jvm.java.net.URI.create(path)
            conf = sc._jsc.hadoopConfiguration()
            fs = jvm.org.apache.hadoop.fs.FileSystem.get(uri, conf)
            return fs.exists(jvm.org.apache.hadoop.fs.Path(path))
        except Exception:
            return False

    def anomaly_flag_static(self, d: DataFrame) -> DataFrame:
        p = self.args.baseline_path
        if not self._hdfs_path_exists(self.spark, p):
            return d.withColumn("z_cases", F.lit(None).cast("double")).withColumn("is_anomaly_cases", F.lit(False))
        try:
            base = (self.spark.read.parquet(p)
                        .select(
                            F.initcap(F.trim(F.coalesce(F.col("country"), F.col("country_name")))).alias("country"),
                            F.col("cases").cast("double").alias("cases")
                        ))
        except Exception:
            return d.withColumn("z_cases", F.lit(None).cast("double")).withColumn("is_anomaly_cases", F.lit(False))
        baseline = (base.groupBy("country")
                        .agg(F.avg("cases").alias("cases_avg"),
                             F.stddev_samp("cases").alias("cases_std")))
        j = d.join(baseline, on="country", how="left")
        z = F.when((F.col("cases_std").isNull()) | (F.col("cases_std") == 0), F.lit(None)) \
              .otherwise((F.col("cases").cast("double") - F.col("cases_avg")) / F.col("cases_std"))
        return j.withColumn("z_cases", z).withColumn("is_anomaly_cases", F.abs(F.col("z_cases")) > F.lit(3.0))

    def write_silver(self, d: DataFrame):
        return (d.writeStream
                  .format("parquet")
                  .outputMode("append")
                  .option("path", self.args.silver)
                  .option("checkpointLocation", f"{self.args.ckpt}/silver")
                  .partitionBy("event_date")
                  .trigger(processingTime="15 seconds")
                  .start())

    def write_region_agg_to_kafka(self, agg: DataFrame):
        payload = (agg.select(F.to_json(F.struct(
                        F.col("region"),
                        F.col("window_start"),
                        F.col("window_end"),
                        F.col("sum_cases"),
                        F.col("sum_deaths"),
                        F.col("sum_tests"),
                        F.col("death_rate_pct")
                    )).alias("value")))
        return (payload.writeStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", self.args.kafka)
                  .option("topic", self.args.out_topic)
                  .option("checkpointLocation", f"{self.args.ckpt}/agg")
                  .outputMode("append")
                  .trigger(processingTime="15 seconds")
                  .start())

    def write_dashboard_parquet(self, d: DataFrame):
        os.makedirs(self.args.dash_dir, exist_ok=True)
        path = os.path.join(self.args.dash_dir, "stream_latest.parquet")
        uri = "file://" + os.path.abspath(path)
        cols = ["country","region","cases","deaths","tests","population",
                "cases_per_million","deaths_per_million","tests_per_thousand",
                "latitude","longitude","event_ts","event_date","flag_url",
                "is_anomaly_cases","z_cases"]
        def sink(batch_df, batch_id: int):
            if batch_df.rdd.isEmpty():
                return
            keep = [c for c in cols if c in batch_df.columns]
            (batch_df.select(*keep)
                     .orderBy(F.col("event_ts").desc())
                     .limit(5000)
                     .write.mode("overwrite")
                     .parquet(uri))
        return (d.writeStream
                  .foreachBatch(sink)
                  .option("checkpointLocation", f"{self.args.ckpt}/dash")
                  .outputMode("append")
                  .trigger(processingTime="5 seconds")
                  .start())

    def write_console_sample(self, d: DataFrame):
        sample = d.select("event_ts","country","cases","deaths","region").limit(10)
        return (sample.writeStream
                      .format("console")
                      .option("truncate", False)
                      .outputMode("append")
                      .trigger(processingTime="15 seconds")
                      .start())

    def run(self):
        src = self.read_kafka_stream()
        clean = self.clean_enrich(src)
        dash_view = self.clean_enrich_for_dashboard(src)
        agg = self.window_region_agg(clean)
        enriched = self.anomaly_flag_static(clean)
        q1 = self.write_silver(clean)
        q2 = self.write_region_agg_to_kafka(agg)
        q3 = self.write_dashboard_parquet(dash_view)
        q4 = self.write_console_sample(clean)
        print("Started silver:", q1.id, q1.name)
        print("Started region_agg_to_kafka:", q2.id, q2.name)
        print("Started dashboard_parquet:", q3.id, q3.name)
        print("Started console_sample:", q4.id, q4.name)
        self.spark.streams.awaitAnyTermination()

def _parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--kafka", default="localhost:9092")
    ap.add_argument("--in_topic", default="covid19_stream")
    ap.add_argument("--out_topic", default="covid19_region_agg")
    ap.add_argument("--startingOffsets", default="latest", choices=["latest","earliest"])
    ap.add_argument("--silver", default="hdfs://localhost:9000/user/student/covid19/stream_silver")
    ap.add_argument("--agg_path", default="hdfs://localhost:9000/user/student/covid19/stream_agg")
    ap.add_argument("--ckpt", default="hdfs://localhost:9000/user/student/checkpoints/task5")
    ap.add_argument("--dash_dir", default=os.path.expanduser("~/de-ass/data/dashboard"))
    ap.add_argument("--baseline_path", default="hdfs://localhost:9000/user/student/covid19/processed")
    return ap.parse_args()

if __name__ == "__main__":
    Task5StreamingPipeline(_parse_args()).run()
