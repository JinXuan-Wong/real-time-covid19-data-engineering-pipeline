===========================================================
BMDS2013 Data Engineering Assignment – README.md
===========================================================

Team Reference: S3G3-2
Submission Date: 29 August 2025

-----------------------------------------------------------
1. Project Title:
-----------------------------------------------------------
Real-Time COVID-19 Outbreak Monitoring (SDG 3 – Good Health and Well-Being)

-----------------------------------------------------------
2. Project Folder Structure: 
-----------------------------------------------------------
de-ass/
  ├── classes/
      ├── __init__.py
      ├── api_producer.py
      ├── kafka_to_hdfs_raw.py
      ├── task2_clean_and_standardise.py
      ├── task2_enrichment.py
      ├── task2_pipeline.py
      ├── task2_validation.py
      ├── task3_mongo_sink.py
      ├── task3_pipeline.py
      ├── task3_queries.py
      ├── task3_reader.py
      ├── task3_transformer.py
      ├── task4_neo4j_loader.py
      ├── task4_neo4j_queries.py
      ├── task5_dashboard.py
      └── task5_streaming.py
  ├── jobs/
      ├── run_task1_producer.sh
      ├── run_task1_stream.sh
      ├── run_task2_batch.sh
      ├── run_task3_pipeline.sh
      ├── run_task3_queries.sh
      ├── run_task4_neo4j.sh
      ├── run_task4_queries.sh
      ├── run_task4_to_neo4j.sh
      └── run_task5_stream.sh
  ├── data/
      ├── covid19/
          ├── task1_raw/
          │   └── raw_sample.csv
          ├── task2_processed/
          │   ├── processed_sample.csv
          │   ├── rejects_sample.csv
          │   └── summary_sample.csv
          └── task5_streaming/
              ├── task5_silver_sample.csv
              └── task5_dashboard_snapshot_sample.csv
      └── dashboard
          └── stream_latest.parquet
  ├── docs/
      ├── task1_checkpoint_dir.png
      ├── task1_raw_dir.png
      ├── task2_processed_dir.txt
      ├── task2_rejects_dir.txt
      ├── task2_schema_preview.txt
      ├── task3_blind_spots_positivity.png
      ├── task3_health_strain.csv
      ├── task3_health_strain.png
      ├── task3_outbreak_blind_spots.csv
      ├── task4_top_cases_per_million.png
      ├── task4_testing_gap.png
      ├── task5_dash_parquet_schema.txt
      └── task5_dashboard_screenshot.png
  ├── notebooks/
      ├── Task2_Validate.ipynb
      └── Task4_Neo4jDEmo.ipynb
  ├── requirements.txt
  └── README.md

-----------------------------------------------------------
3. Links to Data Source: http://disease.sh
-----------------------------------------------------------
(Note: No large data files needed to download) 

This project uses the disease.sh public health API, an open and reliable source 
of free, real-time COVID-19 data in JSON format. 

-----------------------------------------------------------
4. Setup Instructions:
-----------------------------------------------------------
Environment:
  # Recommend using your venv
  source ~/.venvs/de-ass/bin/activate
  export PYSPARK_PYTHON="$VIRTUAL_ENV/bin/python"
  export PYSPARK_DRIVER_PYTHON="$VIRTUAL_ENV/bin/python"
  
  pip install -r requirements.txt
  
==============================================
TASK 1 — Stream Raw Data
==============================================
Prereqs (run as hduser):
  start-dfs.sh
  start-yarn.sh
  zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties &
  kafka-server-start.sh $KAFKA_HOME/config/server.properties &
  kafka-topics.sh --create --topic covid19_stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Prepare HDFS (run once as student):
  hdfs dfs -mkdir -p /user/student/covid19/raw
  hdfs dfs -mkdir -p /user/student/checkpoints/covid19_raw

Run (as student, in ~/de-ass):
  ./jobs/run_task1_producer.sh    # API -> Kafka
  ./jobs/run_task1_stream.sh      # Kafka -> HDFS Parquet

Verify (optional):
  hdfs dfs -ls /user/student/covid19/raw
  hdfs dfs -ls /user/student/checkpoints/covid19_raw
  hdfs dfs -ls /user/student/covid19/raw/event_date=*   # list parquet files

Stop:
  Ctrl+C in the producer and stream terminals.

==============================================
TASK 2 — Process Data
==============================================
Purpose:
  Clean, standardise, enrich, and validate raw data. Write valid rows to processed/
  and invalid rows to rejects/ (partitioned by event_date).

Input (raw):
  hdfs://localhost:9000/user/student/covid19/raw

Outputs:
  Processed:           hdfs://localhost:9000/user/student/covid19/processed
  Rejects (invalid):   hdfs://localhost:9000/user/student/covid19/rejects
  Validation summary:  hdfs://localhost:9000/user/student/covid19/validation_summary

Run (as student, in ~/de-ass):
  ./jobs/run_task2_batch.sh                 # process all available dates
  ./jobs/run_task2_batch.sh 2025-08-xx      # process a specific date only

Verify:
  # 1) Check processed parquet files exist
  hdfs dfs -ls /user/student/covid19/processed

  # 2) Check schema + sample rows 
  $SPARK_HOME/bin/spark-shell <<'EOT'
  val df = spark.read.parquet("hdfs://localhost:9000/user/student/covid19/processed")
  df.printSchema()
  df.select("country","cases","deaths","cases_per_million","event_date").show(10, false)
  System.exit(0)
  EOT

==============================================
TASK 3 — Working with MongoDB
==============================================
Purpose:
  Store Task 2 (cleaned) country-day snapshots as nested documents in MongoDB
  and run analytics queries. PySpark reads from HDFS and writes to MongoDB
  via a custom PyMongo sink.

Inputs:
  hdfs:///user/student/covid19/processed

Mongo:
  Database: covid
  Collection: daily_country

  # HDFS parquet from Task 2
  export TASK2_PARQUET="hdfs:///user/student/covid19/processed"

  # Mongo connection (Atlas or local)
  export MONGODB_URI='mongodb+srv://<user>:<pass>@<cluster>/?retryWrites=true&w=majority'
  # (optional, defaults used by the pipeline)
  export MONGO_DB='covid'
  export MONGO_COLL='daily_country'

Install (once):
  pip install -r requirements.txt
  # (ensures: pyspark, pymongo, pandas, matplotlib, etc.)

Run (load → MongoDB):
  # Uses Spark to read from HDFS and upsert into MongoDB.
  # IMPORTANT: use the script so executors receive the Python modules.
  ./jobs/run_task3_pipeline.sh

Run (queries/report):
  # Produces a small report/plots from MongoDB (uses the same MONGODB_URI).
  ./jobs/run_task3_queries.sh

Verify in MongoDB (mongosh):
  mongosh "$MONGODB_URI"
  use covid
  db.daily_country.countDocuments()
  db.daily_country.findOne()
  # (optional indexes for faster upsert/query)
  db.daily_country.createIndex({"country":1,"timestamps.updated":-1})
  # or if your docs use event_id:
  # db.daily_country.createIndex({event_id: 1}, {unique: true})

==============================================
TASK 4 — Working with Neo4j
==============================================
Purpose:
  Turn the Task 2 cleaned daily country snapshots into a graph and run analytics
  queries from Neo4j. PySpark reads from HDFS; the Neo4j Python driver writes nodes
  + relationships; two Cypher-based charts are saved as PNGs.

Inputs:
  hdfs:///user/student/covid19/processed

Neo4j:
  Works with Neo4j Aura or local Neo4j.

Environment variables:
  NEO4J_URI      — e.g. neo4j+s://<id>.databases.neo4j.io (Aura) or bolt://localhost:7687 (local)
  NEO4J_USER     — usually neo4j
  NEO4J_PASS     — your password
  NEO4J_DATABASE — (optional) default: neo4j
  TASK2_PARQUET  — hdfs:///user/student/covid19/processed

Install:
  # inside your venv
  pip install -r requirements.txt

(Optional) make scripts executable:
  chmod +x jobs/*.sh

Connectivity check:
  export NEO4J_URI='neo4j+s://<your-aura-id>.databases.neo4j.io'
  export NEO4J_USER='neo4j'
  export NEO4J_PASS='<your-password>'
  export NEO4J_DATABASE='neo4j'
  ./jobs/run_task4_neo4j.sh
  
Run (load Task 2 parquet → Neo4j):
  export TASK2_PARQUET='hdfs:///user/student/covid19/processed'
  # Load a single date:
  ./jobs/run_task4_to_neo4j.sh 2025-08-16
  # or load all available dates:
  ./jobs/run_task4_to_neo4j.sh

Run (generate charts):
  ./jobs/run_task4_queries.sh 

  # Requires data already loaded for that date
  ./jobs/run_task4_queries.sh 2025-08-16
  # PNGs are saved to: de-ass/docs/
  #   - task4_top_cases_per_million.png
  #   - task4_testing_gap.png

Verify:
  MATCH (c:Country)-[:HAS_EVENT]->(e:Event) RETURN count(c), count(e);
  CALL db.schema.visualization();

JupyterLab (optional):
  pip install jupyterlab
  cd ~/de-ass
  jupyter lab --no-browser --ip=127.0.0.1

  # In a notebook cell:
  import os
  os.environ["NEO4J_URI"] = "neo4j+s://<your-aura-id>.databases.neo4j.io"
  os.environ["NEO4J_USER"] = "neo4j"
  os.environ["NEO4J_PASS"] = "<your-password>"
  os.environ["NEO4J_DATABASE"] = "neo4j"

  # Then run the query helper as a script:
  %run classes/task4_neo4j_queries.py --date 2025-08-16 --db neo4j --limit 12

==============================================
TASK 5 — Spark Structured Streaming
==============================================
Purpose:
  Read JSON from Kafka topic covid19_stream (from Task 1).
  Clean & enrich (derive event_ts, event_date, region codes, per-capita metrics).
  Three sinks:
      1. HDFS silver Parquet partitioned by event_date.
      2. Kafka regional aggregates → topic covid19_region_agg.
      3. Local Parquet snapshot for Streamlit dashboard at ~/de-ass/data/dashboard/stream_latest.parquet/.

Tech:
  PySpark (Structured Streaming), Kafka, HDFS, Streamlit/Plotly.

Prereqs:
  Task 1 producer is running (API → Kafka) so covid19_stream has data.
  Hadoop/YARN running (NameNode/DataNode/ResourceManager/NodeManager).
  ZooKeeper & Kafka brokers running.
  Kafka topics (create once if auto-create is disabled):
    kafka-topics.sh --create --topic covid19_region_agg --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1

Run:
  ./jobs/run_task5_stream.sh 
  streamlit run classes/task5_dashboard.py --server.address=0.0.0.0

Outputs:
  Silver (HDFS): hdfs://localhost:9000/user/student/covid19/stream_silver/
  Region aggregates (Kafka topic): covid19_region_agg
  Dashboard snapshot (local FS): ~/de-ass/data/dashboard/stream_latest.parquet/
  Console sample: prints (event_ts, country, cases, deaths, region) every ~15s

Dashboard Streamlit browser: 
  http://localhost:8501
  

============================================== 
5. Notes:
============================================== 
Run (WSL):
  pip install jupyterlab
  cd ~/de-ass/notebooks
  jupyter lab --no-browser --ip=127.0.0.1

  - Notebooks only import and visualize; all pipeline logic stays in classes/*.py
  - Do NOT hardcode credentials; use MONGODB_URI, NEO4J_URI, and NEO4J_PASS env var
  - The Plotly deprecation warning about country-name locationmode is harmless.
    If desired, switch to ISO-3 later.

============================================== 
Data Folder
============================================== 
- raw_sample.csv → sample of raw ingested data (Task 1, from HDFS raw/).

- processed_sample.csv → cleaned + enriched valid rows (Task 2, from HDFS processed/).
- rejects_sample.csv → invalid rows flagged by validation (Task 2, from HDFS rejects/).
- summary_sample.csv → validation summary snapshot (Task 2, from HDFS validation_summary/).

- task5_silver_sample.csv → sample from the streaming “silver” sink (Task 5, from HDFS stream_silver/).
- task5_dashboard_snapshot_sample.csv → snapshot used by the Streamlit dashboard (Task 5).

These are small CSV samples for demo/reporting; the full data is stored in HDFS Parquet.
