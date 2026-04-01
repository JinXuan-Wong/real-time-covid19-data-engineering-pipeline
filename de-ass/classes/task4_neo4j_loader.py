# Author: Tan Yen Ping
# TASK: TASK 4 - NEO4J

from __future__ import annotations
import os, argparse
from typing import List, Dict, Any
from neo4j import GraphDatabase
from pyspark.sql import SparkSession, DataFrame, functions as F

class EventModelLoader:
    def __init__(self,
                 parquet_path: str,
                 neo4j_uri: str,
                 neo4j_user: str,
                 neo4j_pass: str,
                 database: str | None = None,
                 batch: int = 1000):
        self.parquet_path = parquet_path
        self.driver = GraphDatabase.driver(neo4j_uri, auth=(neo4j_user, neo4j_pass))
        self.database = database
        self.batch = batch
        self.spark = (SparkSession.builder
                      .appName("Task4-Event-Model-Loader")
                      .config("spark.hadoop.fs.defaultFS", "hdfs://localhost:9000")
                      .getOrCreate())

    def _ensure_col(self, df: DataFrame, name: str) -> F.Column:
        return F.col(name) if name in df.columns else F.lit(None).alias(name)

    def read_processed(self, only_date: str | None) -> DataFrame:
        df = self.spark.read.parquet(self.parquet_path)
        if "event_date" not in df.columns:
            if "updated_ts" in df.columns:
                df = df.withColumn("event_date", F.to_date(F.col("updated_ts")))
            elif "updated" in df.columns:
                df = df.withColumn("updated_ts", F.to_timestamp(F.from_unixtime(F.col("updated")/1000))) \
                       .withColumn("event_date", F.to_date(F.col("updated_ts")))
            else:
                raise RuntimeError("No event_date/updated_ts/updated to derive event_date")
        if only_date:
            df = df.where(F.col("event_date") == F.lit(only_date))
        wanted = [
            "country","iso2","iso3","population","continent","region",
            "cases","todayCases","cases_per_million","casesPerOneMillion",
            "deaths","todayDeaths","deaths_per_million","deathsPerOneMillion",
            "recovered","todayRecovered",
            "tests","tests_per_million","testsPerOneMillion","tests_per_thousand",
            "active","critical",
            "updated_ts","event_date"
        ]
        sel = [self._ensure_col(df, c) for c in wanted]
        df = df.select(*sel)
        df = (df
            .withColumn("cases_pm", F.coalesce(F.col("cases_per_million"), F.col("casesPerOneMillion")))
            .withColumn("deaths_pm", F.coalesce(F.col("deaths_per_million"), F.col("deathsPerOneMillion")))
            .withColumn("tests_pm",  F.coalesce(F.col("tests_per_million"),  F.col("testsPerOneMillion")))
        )
        df = df.withColumn(
            "event_id",
            F.concat_ws("-",
                F.coalesce(F.upper(F.col("iso2")), F.upper(F.col("iso3")), F.col("country")),
                F.col("event_date"))
        )
        return df

    @staticmethod
    def _cypher() -> str:
        return (
            """
            UNWIND $rows AS r
            MERGE (c:Country {name: r.country})
              ON CREATE SET c.iso2=r.iso2, c.iso3=r.iso3, c.population=r.population,
                            c.continent=r.continent, c.region=r.region
              ON MATCH  SET c.iso2=coalesce(r.iso2,c.iso2), c.iso3=coalesce(r.iso3,c.iso3),
                            c.population=coalesce(r.population,c.population),
                            c.continent=coalesce(r.continent,c.continent),
                            c.region=coalesce(r.region,c.region)
            MERGE (e:Event {id: r.event_id})
              ON CREATE SET e.event_date = r.event_date
              ON MATCH  SET e.event_date = r.event_date
            MERGE (c)-[:HAS_EVENT]->(e)
            MERGE (cs:Cases {event_id: r.event_id})
              ON CREATE SET cs.total=r.cases, cs.today=r.todayCases, cs.per_million=r.cases_pm
              ON MATCH  SET cs.total=r.cases, cs.today=r.todayCases, cs.per_million=r.cases_pm
            MERGE (e)-[:HAS_CASES]->(cs)
            MERGE (de:Deaths {event_id: r.event_id})
              ON CREATE SET de.total=r.deaths, de.today=r.todayDeaths, de.per_million=r.deaths_pm
              ON MATCH  SET de.total=r.deaths, de.today=r.todayDeaths, de.per_million=r.deaths_pm
            MERGE (e)-[:HAS_DEATHS]->(de)
            MERGE (rc:Recovered {event_id: r.event_id})
              ON CREATE SET rc.total=r.recovered
              ON MATCH  SET rc.total=r.recovered
            MERGE (e)-[:HAS_RECOVERIES]->(rc)
            MERGE (ts:Tests {event_id: r.event_id})
              ON CREATE SET ts.total=r.tests, ts.per_million=r.tests_pm, ts.per_thousand=r.tests_per_thousand
              ON MATCH  SET ts.total=r.tests, ts.per_million=r.tests_pm, ts.per_thousand=r.tests_per_thousand
            MERGE (e)-[:HAS_TESTS]->(ts)
            MERGE (ac:Active {event_id: r.event_id})
              ON CREATE SET ac.active=r.active, ac.critical=r.critical
              ON MATCH  SET ac.active=r.active, ac.critical=r.critical
            MERGE (e)-[:HAS_STATUS]->(ac)
            """
        )

    def _rows(self, df: DataFrame) -> List[Dict[str, Any]]:
        rows = [r.asDict() for r in df.collect()]
        for r in rows:
            if r.get("event_date") is not None:
                r["event_date"] = str(r["event_date"])
        return rows

    def create_constraints(self):
        stmts = [
            "CREATE CONSTRAINT country_name IF NOT EXISTS FOR (c:Country)  REQUIRE c.name IS UNIQUE",
            "CREATE CONSTRAINT event_id     IF NOT EXISTS FOR (e:Event)    REQUIRE e.id   IS UNIQUE",
            "CREATE CONSTRAINT cases_eid    IF NOT EXISTS FOR (n:Cases)    REQUIRE n.event_id IS UNIQUE",
            "CREATE CONSTRAINT deaths_eid   IF NOT EXISTS FOR (n:Deaths)   REQUIRE n.event_id IS UNIQUE",
            "CREATE CONSTRAINT rec_eid      IF NOT EXISTS FOR (n:Recovered) REQUIRE n.event_id IS UNIQUE",
            "CREATE CONSTRAINT tests_eid    IF NOT EXISTS FOR (n:Tests)    REQUIRE n.event_id IS UNIQUE",
            "CREATE CONSTRAINT active_eid   IF NOT EXISTS FOR (n:Active)   REQUIRE n.event_id IS UNIQUE",
        ]
        with self.driver.session(database=self.database) as sess:
            for s in stmts:
                sess.run(s)

    def load(self, only_date: str | None):
        df = self.read_processed(only_date)
        rows = self._rows(df)
        with self.driver.session(database=self.database) as sess:
            for i in range(0, len(rows), self.batch):
                chunk = rows[i:i+self.batch]
                sess.execute_write(lambda tx: tx.run(self._cypher(), rows=chunk))
                print(f"Upserted {i+len(chunk)} / {len(rows)} rows ...")

    def close(self):
        try:
            self.spark.stop()
        finally:
            self.driver.close()

if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Load Event-centered model into Neo4j from Task-2 Parquet")
    ap.add_argument("--date", help="YYYY-MM-DD to load only one event_date partition")
    ap.add_argument("--db", help="Neo4j database name (Aura often 'neo4j')", default=None)
    args = ap.parse_args()
    parquet = os.getenv("TASK2_PARQUET", "hdfs:///user/student/covid19/processed")
    uri  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd  = os.getenv("NEO4J_PASS", "neo4j")
    loader = EventModelLoader(parquet, uri, user, pwd, database=args.db)
    try:
        loader.create_constraints()
        loader.load(args.date)
    finally:
        loader.close()
