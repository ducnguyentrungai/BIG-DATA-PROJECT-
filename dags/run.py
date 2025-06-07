import os
import logging
import subprocess
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime


def run_main_pipeline():
    """
    Gọi script Python chính để chạy toàn bộ ETL pipeline:
    Yahoo → CSV → Parquet (Staging → Core → Business → Mart) → Superset
    """
    script_path = '/opt/airflow/scripts/main/run_pipeline.py'
    abs_path = os.path.abspath(script_path)
    print(f"📂 Absolute script path: {abs_path}")

    if not os.path.exists(abs_path):
        raise FileNotFoundError(f"❌ Không tìm thấy script: {abs_path}")
    
    logging.info(f"▶️ Đang chạy pipeline: {abs_path}")
    try:
        subprocess.run(["python", abs_path], check=True)
        logging.info("✅ Pipeline chạy thành công")
    except subprocess.CalledProcessError as e:
        logging.error(f"❌ Pipeline thất bại: {e}")
        raise e


# Cấu hình DAG
default_args = {
    'owner': 'trungduc',
    'retries': 1
}

with DAG(
    dag_id='aapl_etl_daily',
    default_args=default_args,
    description='ETL Pipeline: Yahoo → CSV → Parquet (Staging → Core → Business → Mart) → Kafka',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['stock', 'etl', 'pipeline', 'mart']
) as dag:

    run_pipeline_task = PythonOperator(
        task_id='run_main_pipeline',
        python_callable=run_main_pipeline
    )

    run_pipeline_task

