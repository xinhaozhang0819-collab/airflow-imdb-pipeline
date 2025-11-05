from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import sqlalchemy

# ---------- CONFIG ----------
DB_CONN = "postgresql+psycopg2://airflow:airflow@postgres_dw:5432/warehouse"
MOVIE_PATH = "/opt/airflow/data/movies.csv"
RATING_PATH = "/opt/airflow/data/ratings.csv"
MERGED_PATH = "/opt/airflow/data/merged.csv"

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ---------- TASK FUNCTIONS ----------
def ingest_movies(ti=None):
    """Read movies.csv and push shape via XCom."""
    df = pd.read_csv(MOVIE_PATH)
    df = df[["movieId", "title", "genres"]]
    if ti:
        ti.xcom_push(key="movies_shape", value=df.shape)
    return df.to_json()

def ingest_ratings(ti=None):
    """Read ratings.csv and push shape via XCom."""
    df = pd.read_csv(RATING_PATH)
    df = df[["userId", "movieId", "rating"]]
    if ti:
        ti.xcom_push(key="ratings_shape", value=df.shape)
    return df.to_json()

def transform_merge(ti=None):
    """Merge movies & ratings on movieId and write merged.csv."""
    import json
    movies = pd.read_json(ti.xcom_pull(task_ids="ingest_movies"))
    ratings = pd.read_json(ti.xcom_pull(task_ids="ingest_ratings"))
    merged = pd.merge(ratings, movies, on="movieId", how="inner")
    merged.to_csv(MERGED_PATH, index=False)
    if ti:
        ti.xcom_push(key="merged_rows", value=len(merged))

def load_to_postgres(ti=None):
    """Load merged.csv into Postgres table imdb_merged."""
    engine = sqlalchemy.create_engine(DB_CONN)
    df = pd.read_csv(MERGED_PATH)
    df.to_sql("imdb_merged", engine, if_exists="replace", index=False)

def analyze(ti=None):
    """Simple analysis: top 5 titles by average rating (printed to logs)."""
    engine = sqlalchemy.create_engine(DB_CONN)
    df = pd.read_sql("SELECT * FROM imdb_merged", engine)
    summary = (
        df.groupby("title")["rating"]
        .mean()
        .reset_index()
        .sort_values("rating", ascending=False)
    )
    top5 = summary.head(5)
    print("Top 5 rated movies:\n", top5)
    if ti:
        ti.xcom_push(key="top5", value=top5.to_dict())

# ---------- DAG ----------
with DAG(
    dag_id="imdb_pipeline",
    default_args=default_args,
    description="ETL pipeline for IMDB-like dataset (local CSVs)",
    schedule_interval="@daily",
    start_date=datetime(2025, 11, 1),
    catchup=False,
    tags=["imdb", "etl", "postgres"],
) as dag:

    ingest_movies_task = PythonOperator(
        task_id="ingest_movies",
        python_callable=ingest_movies,
    )

    ingest_ratings_task = PythonOperator(
        task_id="ingest_ratings",
        python_callable=ingest_ratings,
    )

    transform_merge_task = PythonOperator(
        task_id="transform_merge",
        python_callable=transform_merge,
    )

    load_task = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
    )

    analyze_task = PythonOperator(
        task_id="analyze",
        python_callable=analyze,
    )

    [ingest_movies_task, ingest_ratings_task] >> transform_merge_task >> load_task >> analyze_task
