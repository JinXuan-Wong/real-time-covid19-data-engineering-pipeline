# Author: Dorcas Lim Yuan Yao (Task 2)

from pyspark.sql import DataFrame
from pyspark.sql.functions import col, length, sum as ssum, lit

class ValidationChecks:
    def split(self, df: DataFrame):
        conds = []
        if "population" in df.columns:
            conds.append(("population_positive", col("population") > 0))
        if "deaths" in df.columns and "cases" in df.columns:
            conds.append(("deaths_le_cases", col("deaths") <= col("cases")))
        if "tests" in df.columns and "cases" in df.columns:
            conds.append(("tests_ge_cases", col("tests") >= col("cases")))
        if "iso2" in df.columns:
            conds.append(("iso2_len_ok", (length(col("iso2")) == 2) | col("iso2").isNull()))
        if "iso3" in df.columns:
            conds.append(("iso3_len_ok", (length(col("iso3")) == 3) | col("iso3").isNull()))
        if "cases" in df.columns and "population" in df.columns:
            conds.append(("cases_le_population", col("cases") <= col("population")))
        if "deaths" in df.columns and "population" in df.columns:
            conds.append(("deaths_le_population", col("deaths") <= col("population")))

        if not conds:
            from pyspark.sql.types import StructType, StructField, LongType
            total = df.count()
            valid_df = df
            invalid_df = df.limit(0)
            summary_df = df.sql_ctx.createDataFrame([(total,)], schema=StructType([StructField("total_rows", LongType(), False)]))
            return valid_df, invalid_df, summary_df

        for name, cond in conds:
            df = df.withColumn(f"rule__{name}", cond)

        rule_cols = [f"rule__{name}" for name, _ in conds]
        from functools import reduce
        valid_cond = reduce(lambda a, b: a & b, [col(c) for c in rule_cols])

        valid_df = df.where(valid_cond).drop(*rule_cols)
        invalid_df = df.where(~valid_cond).drop(*rule_cols)

        agg_exprs = [ssum((~col(c)).cast("int")).alias(c.replace("rule__","violations__")) for c in rule_cols]
        summary_df = df.agg(*agg_exprs).withColumn("total_rows", lit(df.count()))

        return valid_df, invalid_df, summary_df
