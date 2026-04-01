# Author: Low Qing Ying (Task 3)

from __future__ import annotations
import os, csv
from pathlib import Path
from typing import List, Dict, Any

from pymongo import MongoClient

class CovidMongoAnalytics:
    def __init__(self,
                 uri: str | None = None,
                 db_name: str = "covid",
                 coll_name: str = "daily_country",
                 export_dir: str | None = None,
                 save_plots: bool = True):
        self.uri = uri or os.getenv("MONGODB_URI") or "mongodb://localhost:27017/"
        self.client = MongoClient(self.uri, tls="mongodb+srv" in self.uri)
        self.coll = self.client[db_name][coll_name]
        self.export_dir = Path(export_dir or os.getenv("TASK3_EXPORT_DIR", str(Path.home() / "de-ass" / "docs")))
        self.save_plots_flag = save_plots and (os.getenv("TASK3_SAVE_PLOTS", "1") == "1")

    def outbreak_blind_spots(self, top_n: int = 18) -> List[Dict[str, Any]]:
        p = [
            {"$match": {"population": {"$gt": 0}}},
            {"$addFields": {
                "tests_pm": {
                    "$cond": [
                        {"$and": [
                            {"$ne": ["$metrics.tests.per_million", None]},
                            {"$gt": ["$metrics.tests.per_million", 0]}
                        ]},
                        "$metrics.tests.per_million",
                        {
                            "$cond": [
                                {"$and": [
                                    {"$gt": ["$metrics.tests.total", 0]},
                                    {"$gt": ["$population", 0]}
                                ]},
                                {"$multiply": [{"$divide": ["$metrics.tests.total", "$population"]}, 1_000_000]},
                                None
                            ]
                        }
                    ]
                }
            }},
            {"$match": {"metrics.active.total": {"$gt": 5_000}, "tests_pm": {"$ne": None, "$lt": 500_000}}},
            {"$project": {
                "_id": 0,
                "country": 1,
                "population": {"$ifNull": ["$population", 0]},
                "active": {"$ifNull": ["$metrics.active.total", 0]},
                "tests_per_million": "$tests_pm",
                "tests_total": {"$ifNull": ["$metrics.tests.total", 0]},
                "cases_total": {"$ifNull": ["$metrics.cases.total", 0]},
                "cases_per_million": {"$ifNull": ["$metrics.cases.per_million", None]},
                "tests_per_case": {"$cond": [{"$gt": ["$metrics.cases.total", 0]}, {"$divide": ["$metrics.tests.total", "$metrics.cases.total"]}, None]},
                "tests_per_active_case": {"$cond": [{"$gt": ["$metrics.active.total", 0]}, {"$divide": ["$metrics.tests.total", "$metrics.active.total"]}, None]},
                "positivity_est": {"$cond": [{"$gt": ["$metrics.tests.total", 0]}, {"$divide": ["$metrics.cases.total", "$metrics.tests.total"]}, None]},
                "active_per_million": {"$cond": [{"$gt": ["$population", 0]}, {"$multiply": [{"$divide": ["$metrics.active.total", "$population"]}, 1_000_000]}, None]},
                "tpm_over_cpm": {"$cond": [
                    {"$and": [
                        {"$ne": ["$metrics.cases.per_million", None]},
                        {"$gt": ["$metrics.cases.per_million", 0]}
                    ]},
                    {"$divide": ["$tests_pm", "$metrics.cases.per_million"]},
                    None
                ]}
            }},
            {"$addFields": {
                "priority_tier": {
                    "$switch": {
                        "branches": [
                            {"case": {"$or": [
                                {"$and": [{"$ne": ["$tpm_over_cpm", None]}, {"$lt": ["$tpm_over_cpm", 2]}]},
                                {"$and": [{"$ne": ["$positivity_est", None]}, {"$gte": ["$positivity_est", 0.15]}]}
                            ]}, "then": "HIGH"},
                            {"case": {"$or": [
                                {"$and": [{"$ne": ["$tpm_over_cpm", None]}, {"$lt": ["$tpm_over_cpm", 5]}]},
                                {"$and": [{"$ne": ["$positivity_est", None]}, {"$gte": ["$positivity_est", 0.08]}]}
                            ]}, "then": "MEDIUM"}
                        ],
                        "default": "LOW"
                    }
                }
            }},
            {"$sort": {"active": -1}},
            {"$limit": int(top_n)}
        ]
        rows = list(self.coll.aggregate(p))
        self._print_blind_spots(rows)
        return rows

    def health_system_strain(self, top_n: int = 20) -> List[Dict[str, Any]]:
        p = [
            {"$match": {"population": {"$gt": 0}}},
            {"$project": {
                "_id": 0,
                "country": 1,
                "population": {"$ifNull": ["$population", 0]},
                "active_total":  {"$ifNull": ["$metrics.active.total", 0]},
                "deaths_total":  {"$ifNull": ["$metrics.deaths.total", 0]},
                "critical_total": {"$ifNull": ["$metrics.critical.total", 0]},
            }},
            {"$addFields": {
                "icu_pct":    {"$cond": [{"$gt": ["$population", 0]}, {"$multiply": [{"$divide": ["$critical_total", "$population"]}, 100]}, 0]},
                "death_pct":  {"$cond": [{"$gt": ["$population", 0]}, {"$multiply": [{"$divide": ["$deaths_total", "$population"]}, 100]}, 0]},
                "active_pct": {"$cond": [{"$gt": ["$population", 0]}, {"$multiply": [{"$divide": ["$active_total", "$population"]}, 100]}, 0]}
            }},
            {"$addFields": {
                "strain_score_pct": {
                    "$add": [
                        {"$multiply": ["$icu_pct",   0.60]},
                        {"$multiply": ["$death_pct", 0.30]},
                        {"$multiply": ["$active_pct",0.10]}
                    ]
                }
            }},
            {"$addFields": {
                "priority": {
                    "$switch": {
                        "branches": [
                            {"case": {"$or": [{"$gte": ["$strain_score_pct", 5.00]}, {"$gte": ["$icu_pct", 0.05]}]}, "then": "HIGH"},
                            {"case": {"$and": [{"$gte": ["$strain_score_pct", 1.0]}, {"$lt": ["$strain_score_pct", 5.00]}]}, "then": "MEDIUM"}
                        ],
                        "default": "LOW"
                    }
                }
            }},
            {"$sort": {"strain_score_pct": -1}},
            {"$limit": int(top_n)}
        ]
        rows = list(self.coll.aggregate(p))
        self._print_strain(rows)
        return rows

    def export_csv(self, filename: str, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        self.export_dir.mkdir(parents=True, exist_ok=True)
        with open(self.export_dir / filename, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader(); w.writerows(rows)

    def save_plots(self, blind_rows: List[Dict[str, Any]], strain_rows: List[Dict[str, Any]]) -> None:
        if not self.save_plots_flag:
            return
        try:
            import matplotlib.pyplot as plt
        except Exception:
            print("[Task3] matplotlib unavailable; skipping plots")
            return
        self.export_dir.mkdir(parents=True, exist_ok=True)

        if blind_rows:
            plt.figure(figsize=(10, 6))
            cats = [d.get("country","?") for d in blind_rows]
            vals = [(d.get("positivity_est") or 0)*100 for d in blind_rows]
            plt.barh(cats, vals); plt.xlabel("Positivity (%)"); plt.title("Outbreak Blind Spots — Positivity (cum. %)")
            plt.gca().invert_yaxis(); plt.tight_layout()
            plt.savefig(self.export_dir / "task3_blind_spots_positivity.png", dpi=160, bbox_inches="tight")
            plt.close()

        if strain_rows:
            plt.figure(figsize=(10, 6))
            cats = [d.get("country","?") for d in strain_rows]
            vals = [d.get("strain_score_pct",0) for d in strain_rows]
            plt.barh(cats, vals); plt.xlabel("Strain Score (%) = 0.60×ICU% + 0.30×Death% + 0.10×Active%")
            plt.title("Health System Strain Index (Percent)"); plt.gca().invert_yaxis(); plt.tight_layout()
            plt.savefig(self.export_dir / "task3_health_strain.png", dpi=160, bbox_inches="tight")
            plt.close()

    def _print_blind_spots(self, rows: List[Dict[str, Any]]) -> None:
        try:
            from rich.console import Console
            from rich.table import Table
            from rich import box
            console = Console(soft_wrap=False)
            table = Table(title="Outbreak Blind Spots — with Risk & Priority", box=box.SIMPLE_HEAVY, pad_edge=False, expand=False, show_header=True, header_style="bold")
            cols = [("Country","left",35),("😷 Active","right",12),("👥 Active/M","right",11),("🧪 Tests/M","right",11),("⚖️ T/Case","right",9),("⚖️ T/Active","right",11),("🔍 Positivity","right",12),("📈 TPM/CPM","right",10),("🎯 Priority","center",10)]
            for h,j,w in cols: table.add_column(h, justify=j, width=w, no_wrap=True, overflow="ellipsis")
            def f(v, nd=2): return "N/A" if v is None else (f"{v:,.{nd}f}" if isinstance(v,(int,float)) else str(v))
            for d in rows:
                pos = d.get("positivity_est"); pos_s = "N/A" if pos is None else f"{pos*100:.2f}%"
                tier = d.get("priority_tier","LOW"); style = {"HIGH":"bold red","MEDIUM":"yellow","LOW":"green"}.get(tier,"white")
                table.add_row(d.get("country","?"), f"{int(d.get('active',0)):,}", f(d.get("active_per_million"),0), f(d.get("tests_per_million"),0), f(d.get("tests_per_case"),2), f(d.get("tests_per_active_case"),2), pos_s, f(d.get("tpm_over_cpm"),2), f"[{style}]{tier}[/]")
            console.print(table)
        except Exception:
            hdr = ("Country","😷 Active","👥 Active/M","🧪 Tests/M","⚖️ T/Case","⚖️ T/Active","🔍 Positivity","📈 TPM/CPM","🎯 Priority")
            fmt = "{:<22} | {:>12} | {:>11} | {:>11} | {:>9} | {:>11} | {:>12} | {:>10} | {:^10}"
            print("\nQUERY 1: Outbreak Blind Spots — with Risk & Priority"); print(fmt.format(*hdr)); print("-"*128)
            def f(v, nd=2, na="N/A"): return na if v is None else (f"{v:,.{nd}f}" if isinstance(v,(int,float)) else str(v))
            for d in rows:
                pos = d.get("positivity_est"); pos_s = "N/A" if pos is None else f"{pos*100:.2f}%"
                print(fmt.format(d.get("country","?")[:22], f"{int(d.get('active',0)):,}", f(d.get("active_per_million"),0), f(d.get("tests_per_million"),0), f(d.get("tests_per_case"),2), f(d.get("tests_per_active_case"),2), pos_s, f(d.get("tpm_over_cpm"),2), d.get("priority_tier","LOW")))

    def _print_strain(self, rows: List[Dict[str, Any]]) -> None:
        try:
            from rich.console import Console
            from rich.table import Table
            from rich import box
            console = Console()
            table = Table(title="Health System Strain Index (Percentages) — with Priority", box=box.MINIMAL_HEAVY_HEAD)
            for h in ["Country","🏥 ICU%","💀 Death%","😷 Active%","📊 Strain%","🎯 Priority"]:
                table.add_column(h, justify="right" if h != "Country" else "left")
            for d in rows:
                tier = d.get("priority","LOW"); style = {"HIGH":"bold red","MEDIUM":"yellow","LOW":"green"}.get(tier,"white")
                table.add_row(d.get("country","?"), f"{d.get('icu_pct',0):.3f}%", f"{d.get('death_pct',0):.3f}%", f"{d.get('active_pct',0):.3f}%", f"{d.get('strain_score_pct',0):.3f}%", f"[{style}]{tier}[/]")
            console.print(table)
        except Exception:
            print("\nHealth System Strain Index (Percentages) — with Priority")
            print("{:<23} | {:>7} | {:>9} | {:>10} | {:>10} | {:>8}".format("Country","ICU%","Death%","Active%","Strain%","Priority"))
            print("-"*95)
            for d in rows:
                print("{:<29} | {:>7.3f}% | {:>9.3f}% | {:>10.3f}% | {:>10.3f}% | {:>8}".format(
                    d.get('country','?'), d.get('icu_pct',0), d.get('death_pct',0), d.get('active_pct',0), d.get('strain_score_pct',0), d.get('priority','LOW')
                ))

    def run(self, top_blind: int = 50, top_strain: int = 20) -> None:
        print(f"[Task3] Running Mongo queries & exports...\n        uri={self.uri}\n        export_dir={self.export_dir}")
        blind = self.outbreak_blind_spots(top_n=top_blind)
        strain = self.health_system_strain(top_n=top_strain)
        self.export_csv("task3_outbreak_blind_spots.csv", blind)
        self.export_csv("task3_health_strain.csv", strain)
        self.save_plots(blind, strain)
        print(f"CSV exports saved to {self.export_dir}")

if __name__ == "__main__":
    top_blind  = int(os.getenv("TOP_N_BLIND", "15"))
    top_strain = int(os.getenv("TOP_N_STRAIN", "20"))
    app = CovidMongoAnalytics()
    app.run(top_blind=top_blind, top_strain=top_strain)
