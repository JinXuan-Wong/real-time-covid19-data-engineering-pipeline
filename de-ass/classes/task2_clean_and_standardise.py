# Author: Dorcas Lim Yuan Yao (Task 2)

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, initcap, upper, trim, regexp_replace, to_timestamp, from_unixtime, to_date, lit

class CleaningAndStandardisation:
    def __init__(self, essential_cols=None):
        self.essential_cols = essential_cols or ["country", "cases", "deaths", "tests", "population", "event_date"]

    def run(self, df: DataFrame) -> DataFrame:
        for c in self.essential_cols:
            if c in df.columns:
                df = df.filter(col(c).isNotNull())
        if "country" in df.columns:
            df = df.withColumn("country", initcap(trim(regexp_replace(col("country"), "<[^>]*>", ""))))
        if "iso2" in df.columns:
            df = df.withColumn("iso2", upper(trim(col("iso2"))))
        if "iso3" in df.columns:
            df = df.withColumn("iso3", upper(trim(col("iso3"))))
        for c in ["cases","deaths","tests","population","active","critical"]:
            if c in df.columns:
                df = df.filter(col(c) >= 0)
        if "updated" in df.columns:
            df = df.withColumn("updated_ts", to_timestamp(from_unixtime((col("updated")/1000))))
        if "event_date" in df.columns:
            df = df.withColumn("event_date", to_date(col("event_date")))
        if set(["country","event_date"]).issubset(df.columns):
            df = df.dropDuplicates(["country","event_date"])
        else:
            df = df.dropDuplicates()
        return df
