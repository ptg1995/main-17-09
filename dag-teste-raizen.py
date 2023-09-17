# datetime
from datetime import timedelta, datetime
import os
# The DAG object
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from teste_raizen.get_data import extract_data_from_git

##variaveis
github_url = "https://github.com/ptg1995/data-engineering-test/raw/master/assets/vendas-combustiveis-m3.xls"
output_filename = f"data_full.xls"

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
            github_url,os.path.join(os.getcwd(),"dags" ,"bronze_raw", output_filename)
        ),
        dag=dag,
    )
    # transform_file_xlsto_xlsx = BashOperator(
    #     task_id="run_after_loop",
    #     bash_command="libreoffice --headless --invisible --convert-to xlsx ./teste_raizen/outputs/temp1.xls --outdir ./teste_raizen/outputs/"
    # )

# Execução das tasks
get_data_xls
