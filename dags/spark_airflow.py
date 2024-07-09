import os
import sys

# 获取当前文件的父目录的父目录（即 /opt/airflow/）
airflow_home = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

sys.path.append(airflow_home)
import airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

from jobs import extract_job
from jobs import load_job

dag = DAG(
    dag_id="Backblaze_drive_data_stats",
    default_args={
        "owner": "xin",
        "start_date": airflow.utils.dates.days_ago(1)
    },
    schedule_interval="@daily"
)

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract_job.download_and_extract_zip,
    op_kwargs={'skip': 0},  # 是否跳过 extract 阶段
    dag=dag,
)

calculate_daily_summary_data = SparkSubmitOperator(
    task_id="calculate_daily_summary_data",
    conn_id='spark_default',
    application="jobs/calculate_annual_summary_data_job.py",
    dag=dag
)

calculate_annual_summary_data = SparkSubmitOperator(
    task_id="calculate_annual_summary_data",
    conn_id='spark_default',
    application="jobs/calculate_daily_summary_data_job.py",
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data_to_db',
    python_callable=load_job.load_data_to_rdb,
    op_kwargs={'db_url': 'postgresql+psycopg2://airflow:airflow@postgres:5432/airflow'},
    dag=dag,
)

extract_task >> [calculate_daily_summary_data, calculate_annual_summary_data] >> load_task
