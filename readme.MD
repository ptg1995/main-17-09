# Projeto de Engenharia de Dados da Raízen

Este repositório contém um projeto de engenharia de dados da Raízen, com o objetivo de extrair, transformar e carregar dados de vendas de combustíveis a partir de um XLS. O projeto utiliza o Apache Airflow e o Apache Spark para realizar as tarefas de ETL (Extração, Transformação e Carga) dos dados.

## Visão Geral do Projeto

O projeto consiste em um pipeline de dados que realiza as seguintes etapas:

1. **Extração de Dados**: Os dados de vendas de combustíveis são extraídos de um arquivo XLS hospedado no GitHub.
2. **Transformação de Dados**: Os dados são transformados em um formato adequado para análise e são salvos em arquivos Parquet particionados.
3. **Carga de Dados**: Os dados transformados são carregados em uma camada "gold" para análise posterior.

## Tecnologias Utilizadas

O projeto utiliza várias tecnologias e bibliotecas para realizar as tarefas de ETL:

- **Python**: A linguagem de programação principal para desenvolvimento do projeto.
- **Apache Airflow**: Uma plataforma de orquestração de tarefas para agendar e executar as etapas do pipeline de dados.
- **Apache Spark**: Um framework de processamento de big data para realizar transformações complexas nos dados.
- **pandas**: Uma biblioteca Python para manipulação e análise de dados.
- **openpyxl**: Uma biblioteca Python para manipulação de arquivos Excel.
- **pyarrow**: Uma biblioteca Python para manipulação de dados em formato Parquet.

## Estrutura do Projeto

O repositório está organizado da seguinte forma:

- **dags_teste_raizen.py**: O arquivo principal que define o pipeline de tarefas do Apache Airflow.
- **get_data.py**: Contém funções para extrair dados de um arquivo XLS hospedado no GitHub.
- **save_data_silver_raw.py**: Contém funções para salvar os dados extraídos em arquivos XLSX na camada silver.
- **transform_oil_data_silver_raw.py**: Contém funções para transformar e salvar os dados de derivados de petróleo na camada silver.
- **transform_diesel_data_silver_raw.py**: Contém funções para transformar e salvar os dados de diesel na camada silver.
- **final_transform_data.py**: Contém funções para ajustar colunas e tipos de dados e salvar os dados finais em formato Parquet na camada gold.

## Como Executar o Projeto

Para executar o projeto, siga os seguintes passos:



## Observações
