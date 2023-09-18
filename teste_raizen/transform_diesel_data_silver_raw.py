import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
import re

def create_dataframe_diesel_silver(path):
    # Criar uma lista vazia para armazenar os novos dados
    new_data = []
    df = pd.read_excel(path)
    # Iterar pelas linhas da planilha original
    for row in df.iterrows():
        # Extrair informações das colunas
        combustivel = row['COMBUSTÍVEL']
        ano = row['ANO']
        estado = row['ESTADO']
        
        # Iterar pelas colunas de meses
        for mes in df.columns[4:]:
            volume = row[mes]
            # Montar a data no formato "YYYY-MM"
            year_month = f"{ano}-{mes.zfill(2)}"
            #criando a coluna de unidade
            resultado = re.search(r'\((.*?)\)', combustivel)
            if resultado:
                unidade = resultado.group(1)
            else:
                unidade = ""
            # Adicionar os dados à lista
            new_data.append([year_month, estado, combustivel, unidade, volume, date.today()])
    # Criar um novo DataFrame com os dados transformados
    oil_data_unpivot = pd.DataFrame(new_data, columns=['year_month', 'uf', 'product', 'unit', 'volume', 'created_at'])
    return oil_data_unpivot

def transform_save_parquet_diesel_silver_raw(folder_path, parquet_path):
    new_df = create_dataframe_diesel_silver(folder_path)
    table = pa.Table.from_pandas(new_df)
    pq.write_to_dataset(table, root_path=parquet_path,partition_cols=['year_month'])
    return print('Tabela oil_derivates saved!')