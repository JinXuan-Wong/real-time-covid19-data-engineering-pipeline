# Author: Dorcas Lim Yuan Yao (Task 2)

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format
from task2_clean_and_standardise import CleaningAndStandardisation
from task2_enrichment import EnrichmentTransformer
from task2_validation import ValidationChecks

class Task2Pipeline:
    def __init__(self, spark, raw_path, processed_path, rejects_path, summary_path, iso_ref_path=None, date_filter=None):
        self.spark = spark
        self.raw_path = raw_path
        self.processed_path = processed_path
        self.rejects_path = rejects_path
        self.summary_path = summary_path
        self.iso_ref_path = iso_ref_path
        self.date_filter = date_filter

    def run(self):
        df = self.spark.read.parquet(self.raw_path)
        if self.date_filter and "event_date" in df.columns:
            df = df.where(date_format(col("event_date"), "yyyy-MM-dd") == self.date_filter)
        cleaner = CleaningAndStandardisation()
        df1 = cleaner.run(df)
        df1 = df1.persist()
        enricher = EnrichmentTransformer(self.spark, iso_ref_path=self.iso_ref_path or "hdfs://localhost:9000/user/student/covid19/ref/iso_ref.csv")
        df2 = enricher.run(df1)
        df2 = df2.persist()
        validator = ValidationChecks()
        valid_df, invalid_df, summary_df = validator.split(df2)
        if "event_date" in valid_df.columns:
            (valid_df.write.mode("append").partitionBy("event_date").parquet(self.processed_path))
        else:
            (valid_df.write.mode("append").parquet(self.processed_path))
        if invalid_df.limit(1).count() > 0:
            if "event_date" in invalid_df.columns:
                (invalid_df.write.mode("append").partitionBy("event_date").parquet(self.rejects_path))
            else:
                (invalid_df.write.mode("append").parquet(self.rejects_path))
        (summary_df.write.mode("append").parquet(self.summary_path))

def main():
    spark = (SparkSession.builder
             .appName("covid19_task2_pipeline")
             .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    p = argparse.ArgumentParser()
    p.add_argument("--date", default=None)
    p.add_argument("--raw", default="hdfs://localhost:9000/user/student/covid19/raw")
    p.add_argument("--processed", default="hdfs://localhost:9000/user/student/covid19/processed")
    p.add_argument("--rejects", default="hdfs://localhost:9000/user/student/covid19/rejects")
    p.add_argument("--summary", default="hdfs://localhost:9000/user/student/covid19/validation_summary")
    p.add_argument("--iso_ref", default="hdfs://localhost:9000/user/student/covid19/ref/iso_ref.csv")
    args = p.parse_args()
    Task2Pipeline(
        spark=spark,
        raw_path=args.raw,
        processed_path=args.processed,
        rejects_path=args.rejects,
        summary_path=args.summary,
        iso_ref_path=args.iso_ref,
        date_filter=args.date
    ).run()
    spark.stop()

if __name__ == "__main__":
    main()
