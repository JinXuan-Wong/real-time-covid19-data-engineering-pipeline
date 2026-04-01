# Author: Low Qing Ying (Task 3)
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, struct, round as sround, when, lit, to_timestamp 

class Task3DocTransformer:
    def run(self, df: DataFrame) -> DataFrame:
        crit_pm = when((col("population") > 0) & col("critical").isNotNull(),
                       sround(col("critical")/col("population")*1000000, 2)).otherwise(lit(None))
        tests_pm = when(col("tests_per_thousand").isNotNull(), col("tests_per_thousand")*1000).otherwise(lit(None))

        metrics = struct(
            struct(col("cases").alias("total"), col("todayCases").alias("today"),
                   col("cases_per_million").alias("per_million")).alias("cases"),
            struct(col("deaths").alias("total"), col("todayDeaths").alias("today"),
                   col("deaths_per_million").alias("per_million")).alias("deaths"),
            struct(col("tests").alias("total"), col("tests_per_thousand").alias("per_thousand"),
                   tests_pm.alias("per_million")).alias("tests"),
            struct(col("recovered").alias("total")).alias("recovered"),
            struct(col("critical").alias("total"), crit_pm.alias("per_million")).alias("critical"),
            struct(col("active").alias("total")).alias("active")
        )

        ts = struct(
            to_timestamp(col("event_date")).alias("event_date"),
            col("event_ts").alias("event_ts"),
            col("updated_ts").alias("updated")
        )

        base = []
        for c in ["country", "continent", "region", "population"]:
            base.append(col(c) if c in df.columns else lit(None).alias(c))

        return df.select(*base, metrics.alias("metrics"), ts.alias("timestamps"))
