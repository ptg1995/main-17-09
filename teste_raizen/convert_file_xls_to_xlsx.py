import os

os.system(
    "libreoffice --headless --invisible --convert-to xlsx /root/airflow/dags/bronze_raw/data_full.xls --outdir /root/airflow/dags/bronze_raw"
)