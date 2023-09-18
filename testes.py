# import pandas as pd
# import pyarrow as pa
# import pyarrow.parquet as pq
# from datetime import date
# import re

# # parquet_path_diesel = '/root/airflow/dags/gold_raw/diesel_gold_partitioned'

# # df = pd.read_parquet(parquet_path_diesel)

# # print((date.today()))

# folder_path = '/root/airflow/dags/silver_raw/oil_derivative_data.xlsx'
# def create_dataframe_oil_gold(path):
#     # Criar uma lista vazia para armazenar os novos dados
#     new_data = []
#     df = pd.read_excel(path)
#     # Iterar pelas linhas da planilha original
#     for index, row in df.iterrows():
#         # Extrair informações das colunas
#         combustivel = row['COMBUSTÍVEL']
#         ano = row['ANO']
#         estado = row['ESTADO']
        
#         # Iterar pelas colunas de meses
#         for mes in df.columns[4:]:
#             volume = row[mes]
#             # Montar a data no formato "YYYY-MM"
#             year_month = f"{ano}-{mes.zfill(2)}"

#             #extraindo a unidade da coluna produto
#             resultado = re.search(r'\((.*?)\)', combustivel)
#             if resultado:
#                 unidade = resultado.group(1)
#             else:
#                 unidade = ""
#             # Adicionar os dados à lista
#             new_data.append([year_month, estado, combustivel, unidade, volume, date.today()])
#     # Criar um novo DataFrame com os dados transformados
#     oil_data_unpivot = pd.DataFrame(new_data, columns=['year_month', 'uf', 'product', 'unit', 'volume', 'created_at'])
#     return oil_data_unpivot

# df = create_dataframe_oil_gold(folder_path)
# print(df)