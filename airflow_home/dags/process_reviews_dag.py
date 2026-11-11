import string
from datetime import datetime
from pathlib import Path

import pandas as pd
from airflow import DAG, Dataset
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.sensors.filesystem import FileSensor
from airflow.utils.task_group import TaskGroup

BASE_DIR = Path(__file__).resolve().parents[2] / "data"
INPUT_FILE = BASE_DIR / "input" / "reviews.csv"
PROCESSED_FILE = BASE_DIR / "processed" / "reviews_processed.csv"

PROCESSED_DATASET = Dataset(f"file://{PROCESSED_FILE}")


def keep_text_and_punctuation(text):
    return "".join(
        char for char in text
        if char.isalnum() or char.isspace() or char in string.punctuation
    ).strip()


with DAG(
    dag_id="process_reviews",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["innowise"],
) as dag:

    wait_for_file = FileSensor(
        task_id="wait_for_file",
        filepath=str(INPUT_FILE),
        fs_conn_id="fs_default",
        poke_interval=10,
        timeout=60 * 60,
        mode="reschedule",
    )

    @task.branch(task_id="check_file")
    def check_file():
        try:
            df = pd.read_csv(INPUT_FILE)
        except pd.errors.EmptyDataError:
            return "log_empty_file"
        if df.empty: 
            return "log_empty_file"
        return "processing.replace_nulls"

    log_empty_file = BashOperator(
        task_id="log_empty_file",
        bash_command=f'echo "$(date) - plik {INPUT_FILE} jest pusty" | tee -a {BASE_DIR}/empty_file.log',
    )

    with TaskGroup(group_id="processing") as processing:

        @task
        def replace_nulls():
            df = pd.read_csv(INPUT_FILE, dtype=str, keep_default_na=True)
            df = df.fillna("-")
            PROCESSED_FILE.parent.mkdir(parents=True, exist_ok=True)
            df.to_csv(PROCESSED_FILE, index=False)

        @task
        def sort_by_date():
            df = pd.read_csv(PROCESSED_FILE, dtype=str, keep_default_na=False)
            df = df.sort_values("created_date")
            df.to_csv(PROCESSED_FILE, index=False)

        @task(outlets=[PROCESSED_DATASET])
        def clean_content():
            df = pd.read_csv(PROCESSED_FILE, dtype=str, keep_default_na=False)
            df["content"] = df["content"].apply(keep_text_and_punctuation)
            df.to_csv(PROCESSED_FILE, index=False)

        replace_nulls() >> sort_by_date() >> clean_content()

    wait_for_file >> check_file() >> [log_empty_file, processing]
