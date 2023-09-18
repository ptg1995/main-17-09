# from datetime import datetime, timedelta
# from pyspark.sql import SparkSession, functions
# from pyspark import SparkConf

# # conf = SparkConf().set("spark.ui.port", "4041")
# ## criando spark session
# def create_spark_session(app_name="spark_teste_raizen"):
#     spark = SparkSession.builder \
#         .appName(app_name) \
#         .config("spark.executor.memory", "2g") \
#         .config("spark.driver.memory", "1g") \
#         .config("spark.ui.port", "4041")\
#         .getOrCreate()
#     return spark

# spark = create_spark_session()

# parquet_path = "/root/airflow/dags/gold_raw/oil_derivative_gold_partitioned"
# df_oil_derivates= spark.read.format("parquet").load(parquet_path)

# df_oil_derivates.show()