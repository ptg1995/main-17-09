## Transformação dos dados:
## 1 - ajustar a coluna year_month - OK
## 2 - ajustar coluna de product (tirar as unidades do produto)
## 3- ajustar a coluna de unidades - OUTROS
## 4 - ajustar dtypes das colunas
## 5 - Salvar na camada gold

from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, regexp_replace, regexp_extract, date_format


def ajustar_coluna_year_month(spark: SparkSession, path):
    parquet_path = path
    df_origin = spark.read.format("parquet").load(parquet_path)
    month_dict = {
        "Jan": "01",
        "Fev": "02",
        "Mar": "03",
        "Abr": "04",
        "Mai": "05",
        "Jun": "06",
        "Jul": "07",
        "Ago": "08",
        "Set": "09",
        "Out": "10",
        "Nov": "11",
        "Dez": "12",
    }
    # retira os dados com year-TOTAL
    df_transf = df_origin.where(~col("year_month").contains("-TOTAL"))
    # Ajusta a coluna year_month
    for idx_dict, value in month_dict.items():
        df_transf = df_transf.withColumn(
            "year_month", regexp_replace("year_month", idx_dict, value)
        )
    return df_transf


def ajustar_coluna_produto(spark: SparkSession, path_origem):
    df_ym_ajusted = ajustar_coluna_year_month(spark, path_origem)
    df_ym_ajusted = df_ym_ajusted.withColumn(
        "product", regexp_replace("product", "(OUTROS)", "- OUTROS")
    )
    df_ym_ajusted = df_ym_ajusted.withColumn(
        "unit", regexp_extract("product", r"\((.*?)\)", 1)
    )
    df_ym_ajusted = df_ym_ajusted.withColumn(
        "product", regexp_replace("product", r"\(.*\)", "")
    )
    return df_ym_ajusted


def ajustar_dtypes(spark: SparkSession, path_origem):
    df = ajustar_coluna_produto(spark, path_origem)
    df.createOrReplaceTempView("df")
    df_dtypes_ajusted = spark.sql(
        """
                            select cast(year_month as date) as year_month
                                , uf
                                , product
                                , unit
                                , cast(volume as double) as volume
                                , cast(created_at as timestamp) as created_at
                            from
                                df"""
    )
    df_dtypes_ajusted = df_dtypes_ajusted.withColumn(
        "year_month", date_format(col("year_month"), "yyyy-MM")
    )
    df_dtypes_ajusted.show()
    df_dtypes_ajusted.printSchema()
    return df_dtypes_ajusted


def save_transform_table_gold_raw(spark: SparkSession, path_origem, path_destino):
    df = ajustar_dtypes(spark, path_origem)
    df.write.partitionBy("year_month").format("parquet").mode("overwrite").save(
        path_destino
    )
