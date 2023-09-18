
import requests
import os

def create_folders():
    folders = ["bronze_raw","silver_raw","gold_raw"]
    for folder in folders:
        os.makedirs(os.path.join("airflow","dags", folder), exist_ok=True)
        print(f"Criando pasta: {folder}, no folder {os.path.join('airflow','dags', folder)}")
            

def extract_data_from_git(url, output_filename):
    response = requests.get(url)
    if response.status_code == 200:
        create_folders()
        with open(output_filename, 'wb') as xls_file:
            xls_file.write(response.content)
        return True
    else:
        print("Falha no dowload do arquivo.")
        return False