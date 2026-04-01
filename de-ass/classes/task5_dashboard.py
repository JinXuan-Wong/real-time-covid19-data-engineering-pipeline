# Author: Wong Jin Xuan
# TASK 5: Real-Time COVID-19 Dashboard (reads parquet snapshot)

import os, time
from datetime import datetime
import pandas as pd
import streamlit as st
import plotly.express as px

class Task5DashboardApp:
    def __init__(self, parquet_dir: str):
        self.parquet_dir = parquet_dir
        st.set_page_config(page_title="COVID Dashboard – Task 5", layout="wide")

    def _latest_part_info(self):
        path = self.parquet_dir
        if not os.path.exists(path):
            return False, "—", None
        parts = []
        for root, _, files in os.walk(path):
            for f in files:
                if f.endswith(".parquet"):
                    full = os.path.join(root, f)
                    try:
                        sz = os.path.getsize(full)
                        if sz > 0:
                            parts.append((os.path.getmtime(full), full))
                    except OSError:
                        pass
        if not parts:
            return True, "no part files yet", None
        ts = max(t for t, _ in parts)
        return True, datetime.fromtimestamp(ts).strftime("%Y-%m-%d %H:%M:%S"), ts

    def load_latest_df(self) -> pd.DataFrame:
        path = self.parquet_dir
        if not os.path.exists(path):
            return pd.DataFrame()
        try:
            import pyarrow.dataset as ds
            table = ds.dataset(path, format="parquet").to_table()
            return table.to_pandas()
        except Exception:
            pass
        parts = []
        if os.path.isdir(path):
            for root, _, files in os.walk(path):
                for f in files:
                    if f.endswith(".parquet"):
                        full = os.path.join(root, f)
                        try:
                            if os.path.getsize(full) > 0:
                                parts.append((os.path.getmtime(full), full))
                        except OSError:
                            pass
            if not parts:
                return pd.DataFrame()
            newest = sorted(parts, key=lambda x: x[0])[-1][1]
            try:
                return pd.read_parquet(newest)
            except Exception:
                return pd.DataFrame()
        else:
            try:
                return pd.read_parquet(path)
            except Exception:
                return pd.DataFrame()

    @staticmethod
    def fmt_int(x):
        try:
            return f"{int(x):,}"
        except Exception:
            return "—"

    def apply_filters(self, df: pd.DataFrame, search_text: str) -> pd.DataFrame:
        if search_text.strip() and "country" in df.columns:
            s = search_text.strip().lower()
            return df[df["country"].str.lower().str.contains(s, na=False)]
        return df

    def render(self):
        st.markdown("<h1 style='margin-bottom:0'>📊 Real-Time COVID-19 Dashboard</h1>", unsafe_allow_html=True)
        with st.sidebar:
            st.subheader("Controls")
            auto = st.checkbox("Auto-refresh", value=True)
            interval = st.slider("Refresh every (seconds)", 1, 30, 5, 1)
            search_text = st.text_input("🔎 Search country", "")
            with st.expander("Runtime", expanded=False):
                st.write("Parquet directory:", self.parquet_dir)
                st.write("Exists:", os.path.exists(self.parquet_dir))
                try:
                    import pyarrow as pa  # noqa
                    st.write("Parquet engine:", "pyarrow")
                except Exception:
                    st.write("Parquet engine:", "pandas default")
        while True:
            exists, last_mod, _ = self._latest_part_info()
            with st.sidebar.expander("Latest snapshot", expanded=False):
                st.write("Exists:", exists); st.write("Last modified:", last_mod)
            df = self.load_latest_df()
            if not df.empty:
                key_cols = [c for c in ["country", "cases", "deaths", "population"] if c in df.columns]
                if key_cols:
                    df = df.dropna(subset=key_cols)
            st.caption(f"Last snapshot: **{last_mod}**")
            k1, k2, k3, k4 = st.columns(4)
            k1.metric("🌍 Countries", int(df["country"].nunique()) if "country" in df.columns else 0)
            k2.metric("🧪 Total Cases", self.fmt_int(df["cases"].sum() if "cases" in df.columns else 0))
            k3.metric("☠️ Total Deaths", self.fmt_int(df["deaths"].sum() if "deaths" in df.columns else 0))
            if {"deaths", "cases"}.issubset(df.columns) and df["cases"].sum() > 0:
                k4.metric("📉 Avg Death Rate %", f"{(df['deaths'].sum()/df['cases'].sum()*100):.2f}")
            else:
                k4.metric("📉 Avg Death Rate %", "—")
            st.markdown("---")
            tab1, tab2, tab3 = st.tabs(["📈 Overview", "🗺️ Map", "📋 Table"])
            with tab1:
                dfo = self.apply_filters(df, st.session_state.get("search_text", ""))
                c1, c2 = st.columns([2, 1])
                if {"country", "cases"}.issubset(dfo.columns):
                    top = (dfo.groupby("country", as_index=False)["cases"].sum()
                             .sort_values("cases", ascending=False).head(20))
                    top["Rank"] = range(1, len(top) + 1)
                    fig = px.bar(
                        top.sort_values("cases"),
                        x="cases", y="country", orientation="h",
                        color="cases", color_continuous_scale="Blues",
                        hover_data={"Rank": True, "cases": ":,"},
                        title="🏆 Top 20 Countries by Cases",
                        labels={"cases": "Total cases", "country": "Country"},
                    )
                    fig.update_layout(coloraxis_showscale=False, margin=dict(l=10, r=10, t=60, b=10))
                    c1.plotly_chart(fig, use_container_width=True)
                if {"region", "cases"}.issubset(dfo.columns):
                    by_region = (dfo.groupby("region", as_index=False)
                                   .agg(cases=("cases","sum"), deaths=("deaths","sum")))
                    fig_t = px.treemap(
                        by_region, path=["region"], values="cases", color="deaths",
                        color_continuous_scale="Sunset",
                        title="🧭 Region Share (size=cases, color=deaths)",
                        hover_data={"cases": ":,", "deaths": ":,"},
                    )
                    fig_t.update_layout(margin=dict(l=10, r=10, t=60, b=10))
                    c2.plotly_chart(fig_t, use_container_width=True)
            with tab2:
                dfm = self.apply_filters(df, st.session_state.get("search_text", ""))
                if {"country","cases"}.issubset(dfm.columns):
                    map_df = (dfm.groupby(["country","latitude","longitude"], as_index=False)
                                .agg(cases=("cases","sum"),
                                    deaths=("deaths","sum"),
                                    pop=("population","max"),
                                    cpm=("cases_per_million","max"),
                                    dpm=("deaths_per_million","max")))

                    fig_map = px.choropleth(
                        map_df, locations="country", locationmode="country names",
                        color="cases", color_continuous_scale="Turbo",
                        title="🌍 Worldwide Cases (choropleth)",
                        labels={"cases":"Total Cases"},
                        hover_name="country",
                        hover_data={"cases":":,","deaths":":,","cpm":":,.0f","dpm":":,.0f"},
                    )
                    fig_map.update_geos(showcountries=True, countrycolor="gray")
                    fig_map.update_layout(margin=dict(l=10, r=10, t=60, b=10))
                    st.plotly_chart(fig_map, use_container_width=True)
                    geo = map_df.dropna(subset=["latitude","longitude"]).sort_values("cases", ascending=False).head(200)
                    if not geo.empty:
                        fig_bubble = px.scatter_geo(
                            geo, lat="latitude", lon="longitude", size="cases",
                            hover_name="country", hover_data={"cases":":,","deaths":":,"},
                            projection="natural earth", title="🫧 Geo Bubbles – Top 200 by Cases",
                        )
                        fig_bubble.update_layout(margin=dict(l=10, r=10, t=60, b=10))
                        st.plotly_chart(fig_bubble, use_container_width=True)
            with tab3:
                tdf = self.apply_filters(df, st.session_state.get("search_text", ""))
                pref = ["country","region","cases","deaths","tests","population",
                        "cases_per_million","deaths_per_million","tests_per_thousand",
                        "event_ts","event_date"]
                cols = [c for c in pref if c in tdf.columns]
                if cols:
                    agg = (tdf.groupby("country", as_index=False)
                             .agg(region=("region","last"),
                                  cases=("cases","sum"),
                                  deaths=("deaths","sum"),
                                  tests=("tests","sum"),
                                  population=("population","max"),
                                  cases_per_million=("cases_per_million","max"),
                                  deaths_per_million=("deaths_per_million","max"),
                                  tests_per_thousand=("tests_per_thousand","max"))
                             .sort_values("cases", ascending=False))
                    agg.insert(0, "🏅 Rank", range(1, len(agg) + 1))
                    for c in ["cases","deaths","tests","population"]:
                        if c in agg.columns: agg[c] = agg[c].map(lambda x: f"{int(x):,}")
                    for c in ["cases_per_million","deaths_per_million","tests_per_thousand"]:
                        if c in agg.columns: agg[c] = agg[c].map(lambda x: f"{x:,.2f}")
                    st.dataframe(agg, use_container_width=True)
            if not auto:
                break
            time.sleep(interval)
            st.rerun()

if __name__ == "__main__":
    base = os.path.expanduser("~/de-ass")
    parquet_dir = os.path.join(base, "data", "dashboard", "stream_latest.parquet")
    Task5DashboardApp(parquet_dir).render()
