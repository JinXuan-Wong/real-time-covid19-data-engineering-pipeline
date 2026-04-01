# Author: Tan Yen Ping
# TASK: TASK 4 - NEO4J

from __future__ import annotations
import os
import sys
import argparse
from pathlib import Path

try:
    from neo4j import GraphDatabase
    import pandas as pd
    import matplotlib.pyplot as plt
except ImportError as e:
    missing = str(e).split('"')[-2] if '"' in str(e) else str(e)
    print("[ERROR] Missing package:", missing)
    print("Install the dependencies then re-run:")
    print("    python -m pip install -U neo4j pandas matplotlib")
    sys.exit(1)

_IN_NOTEBOOK = False
try:
    from IPython.display import display  
    _IN_NOTEBOOK = bool(getattr(getattr(__import__('IPython'), 'get_ipython')(), 'kernel', None))
except Exception:
    _IN_NOTEBOOK = False


class Neo4jQueries:
    def __init__(self, uri: str, user: str, password: str, database: str | None = None):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
        self.database = database
        self.docs_dir = self._resolve_docs_dir()
        self.docs_dir.mkdir(parents=True, exist_ok=True)

    def _resolve_docs_dir(self) -> Path:
        """Save into de-ass/docs relative to your project root."""
        cwd = Path.cwd()
        if cwd.name == "classes" and cwd.parent.name == "de-ass":
            return cwd.parent / "docs"
        for p in [cwd, *cwd.parents]:
            if p.name == "de-ass":
                return p / "docs"
        return Path("de-ass") / "docs"

    def _count(self, cypher: str) -> int:
        with self.driver.session(database=self.database) as sess:
            rec = sess.run(cypher).single()
            return 0 if rec is None else int(list(rec.values())[0])

    def _has_event_model(self) -> bool:
        return self._count("MATCH (e:Event) RETURN count(e) AS c") > 0

    @staticmethod
    def _format_int(x):
        try:
            return f"{int(x):,}"
        except Exception:
            return "—"

    @staticmethod
    def _format_float(x, digits=2):
        try:
            return f"{float(x):,.{digits}f}"
        except Exception:
            return "—"

    def _print_table(self, title: str, date: str, df: pd.DataFrame, fmt_map: dict, model_label: str, skipped_nulls: int):
        df = df.copy()
        df.insert(0, "#", range(1, len(df) + 1))

        for col, kind in fmt_map.items():
            if col in df.columns:
                if kind == "int":
                    df[col] = df[col].map(self._format_int)
                elif kind == "float2":
                    df[col] = df[col].map(lambda v: self._format_float(v, 2))
                elif kind == "float3":
                    df[col] = df[col].map(lambda v: self._format_float(v, 3))

        header = f"\n{title}\nDate: {date}   Model: {model_label}\n"
        print(header)

        print(df.to_string(index=False, col_space=12))
        print(f"\nShown: {len(df)} rows | Skipped NULLs: {skipped_nulls}\n" + ("-" * 72))

    def query1_cases_per_million(self, date: str, limit: int = 10) -> Path:
        if not self._has_event_model():
            print("[ERROR] Event-centered model not found. Load Task 4 data first.")
            return Path()

        cypher = """
        MATCH (c:Country)-[:HAS_EVENT]->(e:Event {event_date:$date})
        MATCH (e)-[:HAS_CASES]->(cs:Cases)
        WITH c, cs,
             coalesce(
               cs.per_million,
               CASE
                 WHEN c.population IS NULL OR c.population = 0 OR cs.total IS NULL THEN NULL
                 ELSE toFloat(cs.total)/toFloat(c.population)*1000000
               END
             ) AS cpm,
             CASE WHEN cs.per_million IS NULL THEN 'computed' ELSE 'reported' END AS cpm_source
        WHERE cpm IS NOT NULL
        RETURN c.name AS Country,
               c.continent AS Continent,
               c.population AS Population,
               cs.total AS CasesTotal,
               cs.today AS TodayCases,
               cpm AS CasesPerMillion,
               cpm_source AS CpmSource
        ORDER BY CasesPerMillion DESC
        LIMIT $limit
        """

        with self.driver.session(database=self.database) as sess:
            rows = [dict(r) for r in sess.run(cypher, date=date, limit=limit)]

        skipped_nulls = 0 
        if not rows:
            print(f"[WARN] Query 1 returned 0 rows for date={date}. Is your data loaded?")
            return Path()

        df = pd.DataFrame(rows)

        df_chart = df[["Country", "CasesPerMillion"]].iloc[::-1]
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(df_chart["Country"], df_chart["CasesPerMillion"])
        ax.set_xlabel("Cases per million")
        ax.set_ylabel("Country")
        ax.set_title(f"Top {len(df_chart)} countries by cases per million on {date}")
        plt.tight_layout()
        outfile = self.docs_dir / "task4_top_cases_per_million.png"
        fig.savefig(outfile, dpi=180)
        plt.close(fig)
        print(f"[OK] Saved chart → {outfile}")

        fmt_map = {
            "Population": "int",
            "CasesTotal": "int",
            "TodayCases": "int",
            "CasesPerMillion": "float2",
        }
        self._print_table(
            title="Query 1 — Top Cases per Million",
            date=date,
            df=df,
            fmt_map=fmt_map,
            model_label="Event-centered",
            skipped_nulls=skipped_nulls
        )
        return outfile

    def query2_under_testing_gap(self, date: str, limit: int = 10) -> Path:
        if not self._has_event_model():
            print("[ERROR] Event-centered model not found. Load Task 4 data first.")
            return Path()

        cypher = """
        MATCH (c:Country)-[:HAS_EVENT]->(e:Event {event_date:$date})
        MATCH (e)-[:HAS_CASES]->(cs:Cases)
        MATCH (e)-[:HAS_TESTS]->(t:Tests)
        WITH c, cs, t,
             coalesce(
               cs.per_million,
               CASE
                 WHEN c.population IS NULL OR c.population = 0 OR cs.total IS NULL THEN NULL
                 ELSE toFloat(cs.total)/toFloat(c.population)*1000000
               END
             ) AS cpm,
             t.per_thousand AS tpt,
             cs.total AS cases_total,
             t.total AS tests_total
        WITH c, cpm, tpt, cases_total, tests_total,
             CASE WHEN cpm IS NULL OR tpt IS NULL THEN NULL ELSE cpm - (tpt * 1000) END AS gap
        WHERE cpm IS NOT NULL AND tpt IS NOT NULL AND gap IS NOT NULL
        RETURN c.name AS Country,
               c.continent AS Continent,
               c.population AS Population,
               cases_total AS CasesTotal,
               tests_total AS TestsTotal,
               cpm AS CasesPerMillion,
               tpt AS TestsPerThousand,
               gap AS Gap
        ORDER BY Gap DESC
        LIMIT $limit
        """

        with self.driver.session(database=self.database) as sess:
            rows = [dict(r) for r in sess.run(cypher, date=date, limit=limit)]

        skipped_nulls = 0  
        if not rows:
            print(f"[WARN] Query 2 returned 0 rows for date={date}. Is your data loaded & has tests data?")
            return Path()

        df = pd.DataFrame(rows)

        df_chart = df[["Country", "Gap"]].iloc[::-1]
        fig, ax = plt.subplots(figsize=(10, 6))
        ax.barh(df_chart["Country"], df_chart["Gap"])
        ax.set_xlabel("Gap = cases per million − tests per thousand × 1000")
        ax.set_ylabel("Country")
        ax.set_title(f"Under-testing Gap (larger = worse) on {date}")
        plt.tight_layout()
        outfile = self.docs_dir / "task4_testing_gap.png"
        fig.savefig(outfile, dpi=180)
        plt.close(fig)
        print(f"[OK] Saved chart → {outfile}")

        fmt_map = {
            "Population": "int",
            "CasesTotal": "int",
            "TestsTotal": "int",
            "CasesPerMillion": "float2",
            "TestsPerThousand": "float3",
            "Gap": "float2",
        }
        self._print_table(
            title="Query 2 — Under-testing Gap (cpm − tpt×1000)",
            date=date,
            df=df,
            fmt_map=fmt_map,
            model_label="Event-centered",
            skipped_nulls=skipped_nulls
        )
        return outfile

    def close(self):
        self.driver.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description="Task 4 — Pretty tables + charts (Event-centered model)")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD partition (e.g., 2025-08-16)")
    ap.add_argument("--db", default=os.getenv("NEO4J_DATABASE", "neo4j"), help="Neo4j database name")
    ap.add_argument("--limit", type=int, default=12, help="Top-N rows for each table/chart")
    args = ap.parse_args()

    uri  = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    pwd  = os.getenv("NEO4J_PASS", "neo4j")

    q = Neo4jQueries(uri, user, pwd, database=args.db)
    try:
        out1 = q.query1_cases_per_million(args.date, limit=args.limit)
        out2 = q.query2_under_testing_gap(args.date, limit=args.limit)
        if out1:
            print(f"\nSaved: {out1}")
        if out2:
            print(f"Saved: {out2}")
        print("\nDone.")
    finally:
        q.close()
