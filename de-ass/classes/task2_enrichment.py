# Author: Dorcas Lim Yuan Yao (Task 2)

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, round as sround, current_timestamp, lit

class EnrichmentTransformer:
    def __init__(self, spark: SparkSession, iso_ref_path: str = "hdfs://localhost:9000/user/student/covid19/ref/iso_ref.csv"):
        self.spark = spark
        self.iso_ref_path = iso_ref_path

    def run(self, df: DataFrame) -> DataFrame:
        if set(["population","cases"]).issubset(df.columns):
            df = df.withColumn("cases_per_million", sround((col("cases")/col("population"))*1_000_000, 2))
        if set(["population","deaths"]).issubset(df.columns):
            df = df.withColumn("deaths_per_million", sround((col("deaths")/col("population"))*1_000_000, 2))
        if set(["population","tests"]).issubset(df.columns):
            df = df.withColumn("tests_per_thousand", sround((col("tests")/col("population"))*1_000, 2))
        try:
            iso = (self.spark.read.option("header","true").csv(self.iso_ref_path)
                   .selectExpr("upper(trim(iso2)) as iso2_ref","upper(trim(iso3)) as iso3_ref","country_official","who_region","income_group"))
            if "iso2" in df.columns:
                df = df.join(iso, df.iso2 == iso.iso2_ref, "left")
            elif "iso3" in df.columns:
                df = df.join(iso, df.iso3 == iso.iso3_ref, "left")
        except Exception:
            pass
        df = df.withColumn("ingest_time", current_timestamp())
        df = df.withColumn("source_system", lit("disease.sh"))
        return df
