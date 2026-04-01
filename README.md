## 📊 Real-Time COVID-19 Data Engineering Pipeline

### 📌 Overview

This project implements a complete end-to-end data engineering pipeline for real-time COVID-19 outbreak monitoring. It transforms raw streaming data into structured, analyzable formats and supports multiple storage and analytics systems.

The system is designed based on **SDG 3: Good Health and Well-Being**, aiming to provide timely insights into global health trends using scalable big data technologies.

---

### 🚀 Key Features

- 🔄 Real-time data ingestion using Kafka  
- ⚡ Stream & batch processing with Apache Spark  
- 🗄️ Distributed storage using HDFS  
- 📦 NoSQL storage using MongoDB  
- 🔗 Graph analytics using Neo4j  
- 📊 Interactive dashboard using Streamlit  
- ✅ Data validation, cleaning, and enrichment pipelines  

---

### 🏗️ Architecture Overview

The pipeline consists of multiple stages:

#### Data Ingestion
- Fetch real-time COVID-19 data from API (disease.sh)  
- Produce data into Kafka topic  

#### Streaming Layer
- Consume Kafka stream using Spark Structured Streaming  
- Store raw data into HDFS  

#### Batch Processing
- Clean, standardize, and validate data  
- Generate processed datasets and rejected records  

#### Data Storage
- MongoDB: document-based storage for analytics  
- Neo4j: graph-based relationships and queries  

#### Analytics & Visualization
- Spark queries for insights  
- Streamlit dashboard for visualization  

---

## 📂 Project Structure

```text
de-ass/
├── classes/                         # Core pipeline logic (PySpark, Kafka, MongoDB, Neo4j)
│   ├── __init__.py
│   ├── api_producer.py              # Fetch API data → Kafka
│   ├── kafka_to_hdfs_raw.py         # Stream Kafka → HDFS (raw layer)
│   ├── task2_clean_and_standardise.py
│   ├── task2_enrichment.py
│   ├── task2_pipeline.py            # Batch processing pipeline
│   ├── task2_validation.py
│   ├── task3_mongo_sink.py          # Write to MongoDB
│   ├── task3_pipeline.py
│   ├── task3_queries.py             # Analytics queries
│   ├── task3_reader.py
│   ├── task3_transformer.py
│   ├── task4_neo4j_loader.py        # Load graph data into Neo4j
│   ├── task4_neo4j_queries.py
│   ├── task5_dashboard.py           # Streamlit dashboard
│   └── task5_streaming.py           # Spark streaming pipeline
│
├── jobs/                            # Execution scripts (run tasks in sequence)
│   ├── run_task1_producer.sh
│   ├── run_task1_stream.sh
│   ├── run_task2_batch.sh
│   ├── run_task3_pipeline.sh
│   ├── run_task3_queries.sh
│   ├── run_task4_neo4j.sh
│   ├── run_task4_queries.sh
│   ├── run_task4_to_neo4j.sh
│   └── run_task5_stream.sh
│
├── data/                            # Sample datasets (for demo/report only)
│   ├── covid19/
│   │   ├── task1_raw/
│   │   │   └── raw_sample.csv
│   │   ├── task2_processed/
│   │   │   ├── processed_sample.csv
│   │   │   ├── rejects_sample.csv
│   │   │   └── summary_sample.csv
│   │   └── task5_streaming/
│   │       ├── task5_silver_sample.csv
│   │       └── task5_dashboard_snapshot_sample.csv
│   └── dashboard/
│       └── stream_latest.parquet
│
├── docs/                            # Outputs, screenshots & analysis results
│   ├── task1_checkpoint_dir.png
│   ├── task1_raw_dir.png
│   ├── task2_processed_dir.txt
│   ├── task2_rejects_dir.txt
│   ├── task2_schema_preview.txt
│   ├── task3_blind_spots_positivity.png
│   ├── task3_health_strain.csv
│   ├── task3_health_strain.png
│   ├── task3_outbreak_blind_spots.csv
│   ├── task4_top_cases_per_million.png
│   ├── task4_testing_gap.png
│   ├── task5_dash_parquet_schema.txt
│   └── task5_dashboard_screenshot.png
│
├── notebooks/                       # Jupyter notebooks for validation & demo
│   ├── Task2_Validate.ipynb
│   └── Task4_Neo4jDEmo.ipynb
│
└── requirements.txt                 # Python dependencies
```

## ⚙️ Technologies Used

- Python  
- Apache Spark (PySpark)  
- Apache Kafka  
- Hadoop (HDFS, YARN)  
- MongoDB  
- Neo4j  
- Streamlit  
- Pandas / Matplotlib  

---

## 📡 Data Source

- Public API: `http://disease.sh`  

This project uses the **disease.sh** public health API, an open and reliable source of free, real-time COVID-19 data in JSON format.

---

## ▶️ Setup Instructions

### Environment

It is recommended to use a virtual environment:

```bash
source ~/.venvs/de-ass/bin/activate
export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
export PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python"

pip install -r requirements.txt
```
## TASK 1 — Stream Raw Data

### Purpose

Fetch real-time COVID-19 data from the API, publish it to Kafka, and stream it into HDFS as raw Parquet files.

### Prerequisites

Run as `hduser`:

```bash
start-dfs.sh
start-yarn.sh
zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
kafka-server-start.sh $KAFKA_HOME/config/server.properties &
kafka-topics.sh --create --topic covid19_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### Prepare HDFS

Run once as `student`:

```bash
hdfs dfs -mkdir -p /user/student/covid19/raw
hdfs dfs -mkdir -p /user/student/checkpoints/covid19_raw
```

### Run

As student, inside `~/de-ass`:

```bash
./jobs/run_task1_producer.sh
./jobs/run_task1_stream.sh
```

### Verify
```bash
hdfs dfs -ls /user/student/covid19/raw
hdfs dfs -ls /user/student/checkpoints/covid19_raw
hdfs dfs -ls /user/student/covid19/raw/event_date=*
```

### Stop
Press Ctrl + C in the producer and streaming terminals.

## TASK 2 — Process Data
### Purpose

Clean, standardize, enrich, and validate raw data. Valid rows are written to processed/ and invalid rows to rejects/, partitioned by event_date.

### Input
`hdfs://localhost:9000/user/student/covid19/raw`

### Outputs
- Processed: hdfs://localhost:9000/user/student/covid19/processed
- Rejects: hdfs://localhost:9000/user/student/covid19/rejects
- Validation summary: hdfs://localhost:9000/user/student/covid19/validation_summary
  
### Run

```bash
./jobs/run_task2_batch.sh
./jobs/run_task2_batch.sh 2025-08-xx
```

### Verify

```bash
hdfs dfs -ls /user/student/covid19/processed
```
```bash
$SPARK_HOME/bin/spark-shell <<'EOT'
val df = spark.read.parquet("hdfs://localhost:9000/user/student/covid19/processed")
df.printSchema()
df.select("country","cases","deaths","cases_per_million","event_date").show(10, false)
System.exit(0)
EOT
```

## TASK 3 — Working with MongoDB
### Purpose

Store Task 2 cleaned country-day snapshots as nested documents in MongoDB and run analytics queries.

### Input
`hdfs:///user/student/covid19/processed`

### MongoDB Target
- Database: covid
- Collection: daily_country

### Environment Variables
```bash
export TASK2_PARQUET="hdfs:///user/student/covid19/processed"
export MONGODB_URI='mongodb+srv://<user>:<pass>@<cluster>/?retryWrites=true&w=majority'
export MONGO_DB='covid'
export MONGO_COLL='daily_country'
```

### Install
`pip install -r requirements.txt`

### Run

Load data into MongoDB:

`./jobs/run_task3_pipeline.sh`

Run analytics queries and reports:

`./jobs/run_task3_queries.sh`

### Verify in MongoDB
```bash
mongosh "$MONGODB_URI"
use covid
db.daily_country.countDocuments()
db.daily_country.findOne()
db.daily_country.createIndex({"country":1,"timestamps.updated":-1})
```

## TASK 4 — Working with Neo4j
### Purpose

Transform cleaned country-day snapshots into a graph model and run graph analytics in Neo4j.

### Input

`hdfs:///user/student/covid19/processed`

### Environment Variables
```bash
export NEO4J_URI='neo4j+s://<your-aura-id>.databases.neo4j.io'
export NEO4J_USER='neo4j'
export NEO4J_PASS='<your-password>'
export NEO4J_DATABASE='neo4j'
export TASK2_PARQUET='hdfs:///user/student/covid19/processed'
```

### Install
```bash
pip install -r requirements.txt
chmod +x jobs/*.sh
```

### Connectivity Check

`./jobs/run_task4_neo4j.sh`

### Run

Load one date:

`./jobs/run_task4_to_neo4j.sh 2025-08-16`

Load all available dates:

`./jobs/run_task4_to_neo4j.sh`

Generate charts:

```bash
./jobs/run_task4_queries.sh
./jobs/run_task4_queries.sh 2025-08-16
```

Generated PNG files are saved in `de-ass/docs/`:

- `task4_top_cases_per_million.png`
- `task4_testing_gap.png`

### Verify

```bash
MATCH (c:Country)-[:HAS_EVENT]->(e:Event) RETURN count(c), count(e);
CALL db.schema.visualization();
```

### Optional: JupyterLab

```bash
pip install jupyterlab
cd ~/de-ass
jupyter lab --no-browser --ip=127.0.0.1
```

Example notebook setup:
```bash
import os
os.environ["NEO4J_URI"] = "neo4j+s://<your-aura-id>.databases.neo4j.io"
os.environ["NEO4J_USER"] = "neo4j"
os.environ["NEO4J_PASS"] = "<your-password>"
os.environ["NEO4J_DATABASE"] = "neo4j"
```

Then run:
```bash
%run classes/task4_neo4j_queries.py --date 2025-08-16 --db neo4j --limit 12
```

## TASK 5 — Spark Structured Streaming
### Purpose

Read JSON from the Kafka topic covid19_stream, clean and enrich the data, and write results to multiple sinks for analytics and dashboarding.

### Outputs

- HDFS silver Parquet partitioned by event_date
- Kafka regional aggregates to topic covid19_region_agg
- Local Parquet snapshot for Streamlit dashboard at ~/de-ass/data/dashboard/stream_latest.parquet/
  
### Tech Stack

- PySpark Structured Streaming
- Kafka
- HDFS
- Streamlit / Plotly
  
### Prerequisites
- Task 1 producer is running
- Hadoop and YARN are running
- ZooKeeper and Kafka brokers are running

Create the Kafka topic if needed:

`kafka-topics.sh --create --topic covid19_region_agg --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1`

### Run

```bash
./jobs/run_task5_stream.sh
streamlit run classes/task5_dashboard.py --server.address=0.0.0.0
```

### Output Locations

- Silver (HDFS): hdfs://localhost:9000/user/student/covid19/stream_silver/
- Region aggregates (Kafka): covid19_region_agg
- Dashboard snapshot: ~/de-ass/data/dashboard/stream_latest.parquet/
  
### Dashboard

Open in browser:

`http://localhost:8501`

---
## 📁 Data Folder

The `data/` folder contains small sample files for demonstration and reporting:

- `raw_sample.csv` — sample raw ingested data from Task 1
- `processed_sample.csv` — cleaned and enriched valid rows from Task 2
- `rejects_sample.csv` — invalid rows flagged during validation
- `summary_sample.csv` — validation summary snapshot
- `task5_silver_sample.csv` — sample from the Task 5 streaming silver sink
- `task5_dashboard_snapshot_sample.csv` — snapshot used by the Streamlit dashboard

These are lightweight CSV samples only. The full production data is stored in HDFS as Parquet.

---

## 📊 Sample Outputs
### Example Visual Outputs

#### Task 1 — Raw and checkpoint storage

- docs/task1_raw_dir.png
- docs/task1_checkpoint_dir.png

#### Task 3 — MongoDB analytics

- docs/task3_health_strain.png
- docs/task3_blind_spots_positivity.png

#### Task 4 — Neo4j graph analytics

- docs/task4_top_cases_per_million.png
- docs/task4_testing_gap.png

#### Task 5 — Dashboard output

- docs/task5_dashboard_screenshot.png

![Task 3 Health Strain Analysis](docs/task3_health_strain.png)
![Task 4 Top Cases per Million](docs/task4_top_cases_per_million.png)
![Task 5 Dashboard](docs/task5_dashboard_screenshot.png)

---

## 🧠 Key Learning Outcomes
Designing scalable data pipelines
Working with real-time streaming systems
Integrating multiple data storage technologies
Handling data validation and transformation
Building data-driven dashboards

---

## 📌 Notes
- This repository contains sample datasets only, not the full HDFS Parquet data
- Notebooks are used only for validation and visualization; all core pipeline logic is implemented in classes/*.py
- Credentials should not be hardcoded; use environment variables such as MONGODB_URI, NEO4J_URI, and NEO4J_PASS
- The Plotly deprecation warning related to country-name locationmode is harmless and can be replaced with ISO-3 codes later if needed
- Full technical setup is also documented in de-ass/README.md

--- 

## 👨‍💻 Author
Developed as part of Data Engineering coursework.

