# Author: Low Qing Ying (Task 3)

import argparse, os
from pyspark.sql import SparkSession
from classes.task3_reader import ProcessedDatasetReader
from classes.task3_transformer import Task3DocTransformer
from classes.task3_mongo_sink import MongoSink

class Task3Pipeline:
    def __init__(self, spark: SparkSession, parquet_path: str, mongo_uri: str, mongo_db: str, mongo_coll: str):
        self.spark = spark
        self.parquet_path = parquet_path
        self.mongo_uri = mongo_uri
        self.mongo_db = mongo_db
        self.mongo_coll = mongo_coll
    def run(self):
        df = ProcessedDatasetReader(self.spark, self.parquet_path).read()
        docs_df = Task3DocTransformer().run(df)
        sink = MongoSink(self.mongo_uri, self.mongo_db, self.mongo_coll)
        sink.upsert_by_country_updated(docs_df)
        sink.ensure_indexes()
        sink.dedupe_keep_latest_per_country()

def main():
    p = argparse.ArgumentParser()
    p.add_argument("--parquet", default="hdfs://localhost:9000/user/student/covid19/processed")
    p.add_argument("--mongo_uri", default=os.getenv("MONGODB_URI", "mongodb://localhost:27017/"))
    p.add_argument("--mongo_db", default=os.getenv("MONGO_DB", "covid"))
    p.add_argument("--mongo_coll", default=os.getenv("MONGO_COLL", "daily_country"))
    args = p.parse_args()
    spark = (SparkSession.builder
             .appName("covid19_task3_pipeline")
             .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    Task3Pipeline(spark, args.parquet, args.mongo_uri, args.mongo_db, args.mongo_coll).run()
    spark.stop()

if __name__ == "__main__":
    main()
