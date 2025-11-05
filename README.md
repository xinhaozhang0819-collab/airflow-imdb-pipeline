# Airflow IMDB Pipeline

A minimal ETL pipeline orchestrated by **Apache Airflow** that:
1) ingests two related datasets (`movies.csv`, `ratings.csv`),
2) transforms & merges them,
3) loads the result into **PostgreSQL**,
4) runs a simple analysis (top-5 movies by average rating).

## Project Layout
```bash
airflow-imdb-pipeline/
├─ dags/
│ └─ imdb_pipeline.py
├─ data
│ ├─ movies.csv
│ └─ ratings.csv
├─ docker-compose.yml
├─ requirements.txt
└─ README.md
```
## Quickstart

### 1) Start services
```bash
docker compose up -d
```
Postgres will be exposed on host 5433.
Airflow Web UI will be at http://localhost:8080 \
Username: airflow \
Password: airflow \

### 2) Running the IMDB ETL Pipeline

- After logging in, you will see the DAG named imdb_pipeline on the main page.
- The DAG is initially paused. Toggle the switch on the left to turn it on (it should turn blue).
- Click the ▶️ (Play) button on the right to trigger the DAG manually.
- Wait until all tasks turn green — indicating success.
You can view progress in Graph View or Tree View.

#### Once completed:
- The merged dataset will be stored in PostgreSQL (warehouse database). You can check it in your data folder.
- You will be able to see outputs by clicking `analyze` and then click `Logs`. Then you will see something like this: 

![Airflow DAG Screenshot](photo1.png)

![Airflow DAG Screenshot](photo2.png)




### Notes

- Executor: ```SequentialExecutor``` (simple & works well for single-node demos).

- Database connection: ```AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=postgresql+psycopg2://airflow:airflow@postgres_dw:5432/warehouse```

- Also sets ```AIRFLOW_CONN_WAREHOUSE``` so tasks/operators can reference the connection ID ```warehouse```.

### Troubleshooting

- Can't open :8080 → check `docker compose ps` has port `0.0.0.0:8080->8080/tcp`, then `docker compose logs airflow`.

- DB init error → run inside the service:
`docker compose run --rm airflow airflow db init`

- SQLite + LocalExecutor conflict → ensure env var is `AIRFLOW__DATABASE__SQL_ALCHEMY_CONN` (not `CORE__SQL_ALCHEMY_CONN`) and keep `SequentialExecutor` or switch to Postgres for LocalExecutor.