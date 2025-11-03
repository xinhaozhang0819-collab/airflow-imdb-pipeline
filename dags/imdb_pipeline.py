from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import pandas as pd
from sqlalchemy import create_engine
import os, gzip, shutil, urllib.request

DATA_DIR = "/opt/airflow/data"
RAW = f"{DATA_DIR}/raw"
PROC = f"{DATA_DIR}/processed"
OUT = f"{DATA_DIR}/outputs"

default_args = dict(retries=1, retry_delay=timedelta(minutes=2))

def _ensure_dirs():
    for p in [RAW, PROC, OUT]:
        os.makedirs(p, exist_ok=True)

def download(url: str, out_path: str) -> str:
    """下载并（若需要）解压，返回文件路径（仅通过 XCom 传‘路径’）"""
    _ensure_dirs()
    tmp = out_path + (".gz" if url.endswith(".gz") else ".tmp")
    urllib.request.urlretrieve(url, tmp)
    if tmp.endswith(".gz"):
        with gzip.open(tmp, "rb") as fin, open(out_path, "wb") as fout:
            shutil.copyfileobj(fin, fout)
        os.remove(tmp)
    else:
        os.rename(tmp, out_path)
    return out_path

def transform_basics(in_path: str, out_path: str) -> str:
    df = pd.read_csv(in_path, sep="\t", na_values="\\N", low_memory=False)
    df = df[df["titleType"].isin(["movie","tvMovie"])][
        ["tconst","primaryTitle","startYear","runtimeMinutes","genres"]
    ]
    df.to_csv(out_path, index=False)
    return out_path

def transform_ratings(in_path: str, out_path: str) -> str:
    df = pd.read_csv(in_path, sep="\t", na_values="\\N", low_memory=False)
    df = df.rename(columns={"averageRating":"rating","numVotes":"votes"})
    df.to_csv(out_path, index=False)
    return out_path

def merge_and_load(basics_path: str, ratings_path: str, table: str) -> None:
    engine = create_engine("postgresql+psycopg2://airflow:airflow@postgres_dw:5432/warehouse")
    b = pd.read_csv(basics_path)
    r = pd.read_csv(ratings_path)
    m = b.merge(r, on="tconst", how="inner")
    m.to_sql(table, engine, if_exists="replace", index=False)

    # 简单分析：每年平均评分（限制 votes>10k 以稳定）
    top = (
        m[m["votes"] > 10000]
        .assign(year=pd.to_numeric(m["startYear"], errors="coerce"))
        .dropna(subset=["year"])
        .groupby("year")["rating"].mean()
        .reset_index()
        .sort_values("year")
    )
    ax = top.plot(x="year", y="rating", title="Avg rating by year (votes>10k)")
    fig = ax.get_figure()
    fig.savefig(f"{OUT}/avg_rating_by_year.png")
    fig.clf()

with DAG(
    dag_id="imdb_etl",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["imdb","parallel","etl"],
) as dag:

    make_dirs = PythonOperator(
        task_id="make_dirs",
        python_callable=_ensure_dirs
    )

    # --- 提取（并行下载） ---
    with TaskGroup("extract", tooltip="parallel downloads"):
        dl_basics = PythonOperator(
            task_id="download_basics",
            python_callable=download,
            op_kwargs={
                "url": "https://datasets.imdbws.com/title.basics.tsv.gz",
                "out_path": f"{RAW}/title.basics.tsv",
            },
        )
        dl_ratings = PythonOperator(
            task_id="download_ratings",
            python_callable=download,
            op_kwargs={
                "url": "https://datasets.imdbws.com/title.ratings.tsv.gz",
                "out_path": f"{RAW}/title.ratings.tsv",
            },
        )

    # --- 转换（并行清洗） ---
    with TaskGroup("transform", tooltip="clean separately"):
        tf_basics = PythonOperator(
            task_id="transform_basics",
            python_callable=transform_basics,
            op_kwargs={
                "in_path": f"{RAW}/title.basics.tsv",
                "out_path": f"{PROC}/basics.csv",
            },
        )
        tf_ratings = PythonOperator(
            task_id="transform_ratings",
            python_callable=transform_ratings,
            op_kwargs={
                "in_path": f"{RAW}/title.ratings.tsv",
                "out_path": f"{PROC}/ratings.csv",
            },
        )

    # --- 建表（或确保存在） ---
    create_table = PostgresOperator(
        task_id="create_table",
        postgres_conn_id="warehouse",
        sql="""
        CREATE TABLE IF NOT EXISTS imdb_titles (
          tconst TEXT PRIMARY KEY,
          primaryTitle TEXT,
          startYear TEXT,
          runtimeMinutes TEXT,
          genres TEXT,
          rating FLOAT,
          votes INT
        );
        """,
    )

    # --- 合并 + 落库 + 简单分析图 ---
    merge_load = PythonOperator(
        task_id="merge_and_load",
        python_callable=merge_and_load,
        op_kwargs={
            "basics_path": f"{PROC}/basics.csv",
            "ratings_path": f"{PROC}/ratings.csv",
            "table": "imdb_titles",
        },
    )

    # --- 清理中间产物 ---
    cleanup = BashOperator(
        task_id="cleanup_intermediate",
        bash_command=f"rm -f {RAW}/* {PROC}/* || true",
    )

    make_dirs >> [dl_basics, dl_ratings]
    [dl_basics, dl_ratings] >> [tf_basics, tf_ratings]
    [tf_basics, tf_ratings] >> create_table >> merge_load >> cleanup
