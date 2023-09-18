# datetime
from datetime import timedelta, datetime
import os

# The DAG object
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from teste_raizen.get_data import extract_data_from_git
from teste_raizen.save_data_silver_raw import save_tables_silver_raw
from teste_raizen.transform_oil_data import transform_save_parquet_gold_raw
##variaveis
github_url = "https://github.com/ptg1995/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls"
output_filename = "data_full.xls"

path_data_full_bronze = "/root/airflow/dags/bronze_raw/data_full.xlsx"
sheets_data_full = {"DPCache_m3": "oil_derivative", "DPCache_m3_2": "diesel"}

folder_path = '/root/airflow/dags/silver_raw/oil_derivative_data.xlsx'
parquet_path = '/root/airflow/dags/gold_raw/oil_derivative_gold_partitioned'

##DAG
with DAG(
    dag_id="AAATeste-raizen",
    start_date=datetime(2021, 1, 1),
    schedule="@daily",
    description="Teste para eng de dados raízen",
) as dag:
    # cria_pasta_outputs = BashOperator(task_id= 'cria_pasta_outputs',)
    # Primeira task
    get_data_xls = PythonOperator(
        task_id="get_data_xls",
        python_callable=lambda: extract_data_from_git(
            github_url, os.path.join(os.getcwd(),"airflow","dags", "bronze_raw", output_filename)
        ),
        dag=dag,
    )
    transform_file_xlsto_xlsx = BashOperator(
        task_id="transform_file_xlsto_xlsx",
        bash_command="libreoffice --headless --invisible --convert-to xlsx /root/airflow/dags/bronze_raw/data_full.xls --outdir /root/airflow/dags/bronze_raw",
    )
    save_tables_silver_raw_task = PythonOperator(
        task_id="save_tables_silver_raw_task",
        python_callable=lambda: save_tables_silver_raw(
            path_data_full_bronze, sheets_data_full
        ),
        dag=dag,
    )
    save_oil_derivates_gold_raw = PythonOperator(
        task_id="save_oil_derivates_gold_raw",
        python_callable=lambda: transform_save_parquet_gold_raw(
            folder_path,parquet_path
        ),
        dag=dag,
    )

# Execução das tasks
get_data_xls >> transform_file_xlsto_xlsx >> save_tables_silver_raw_task >> save_oil_derivates_gold_raw