import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def create_dataframe_oil_gold(path):
    # Criar uma lista vazia para armazenar os novos dados
    new_data = []
    df = pd.read_excel(path)
    # Iterar pelas linhas da planilha original
    for index, row in df.iterrows():
        # Extrair informações das colunas
        combustivel = row['COMBUSTÍVEL']
        ano = row['ANO']
        estado = row['ESTADO']
        
        # Iterar pelas colunas de meses
        for mes in df.columns[4:]:
            volume = row[mes]
            # Montar a data no formato "YYYY-MM"
            year_month = f"{ano}-{mes.zfill(2)}"
            # Adicionar os dados à lista
            new_data.append([year_month, estado, combustivel, 'm3', volume, pd.Timestamp.now()])
    # Criar um novo DataFrame com os dados transformados
    oil_data_unpivot = pd.DataFrame(new_data, columns=['year_month', 'uf', 'product', 'unit', 'volume', 'created_at'])
    return oil_data_unpivot

def transform_save_parquet_gold_raw(folder_path, parquet_path):
    new_df = create_dataframe_oil_gold(folder_path)
    table = pa.Table.from_pandas(new_df)
    pq.write_to_dataset(table, root_path=parquet_path, partition_cols=['year_month'])
    return print('Tabela oil_derivates saved!')