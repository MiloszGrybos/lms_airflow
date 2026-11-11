from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.providers.mongo.hooks.mongo import MongoHook

PROCESSED_FILE = Path(__file__).resolve().parents[2] / "data" / "processed" / "reviews_processed.csv"
PROCESSED_DATASET = Dataset(f"file://{PROCESSED_FILE}")

MONGO_CONN_ID = "mongo_default"
MONGO_DB = "reviews_db"
MONGO_COLLECTION = "reviews"

with DAG(
    dag_id="load_reviews_to_mongo",
    start_date=datetime(2024, 1, 1),
    schedule=[PROCESSED_DATASET],
    catchup=False,
    tags=["innowise"],
) as dag:

    @task
    def load_to_mongo():
        df = pd.read_csv(PROCESSED_FILE, dtype=str, keep_default_na=False)

        records = df.to_dict("records")
        for rec in records:
            score = pd.to_numeric(rec["score"], errors="coerce")
            if not pd.isna(score):
                rec["score"] = int(score)
            parsed_date = pd.to_datetime(rec["created_date"], errors="coerce")
            if not pd.isna(parsed_date):
                rec["created_date"] = parsed_date.to_pydatetime()

        hook = MongoHook(mongo_conn_id=MONGO_CONN_ID)
        hook.delete_many(MONGO_COLLECTION, {}, mongo_db=MONGO_DB)
        hook.insert_many(MONGO_COLLECTION, records, mongo_db=MONGO_DB)
        print(f"Załadowano {len(records)} dokumentów do {MONGO_DB}.{MONGO_COLLECTION}")

    load_to_mongo()
