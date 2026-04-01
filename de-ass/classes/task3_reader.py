# Author: Low Qing Ying (Task 3)
from pyspark.sql import SparkSession, DataFrame

class ProcessedDatasetReader:
    def __init__(self, spark: SparkSession, parquet_path: str = "hdfs://localhost:9000/user/student/covid19/processed"):
        self.spark = spark
        self.parquet_path = parquet_path
    def read(self) -> DataFrame:
        return self.spark.read.parquet(self.parquet_path)
