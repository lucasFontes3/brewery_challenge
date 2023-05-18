from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
import requests
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json 

# Função que será executada pelo PythonOperator
def run_pyspark_script():
    spark = SparkSession.builder \
            .appName("Brewery_challenge") \
            .getOrCreate()
            
    # Código do seu script em PySpark
    url = "https://api.openbrewerydb.org/v1/breweries"

    # Realiza uma requisição GET para a API e armazena a resposta na variável 'response'
    response = requests.get(url)
                     
                                  

# Verifica se a resposta foi bem sucedida (status code 200 indica sucesso)
if response.status_code == 200:
    # Se a resposta foi bem sucedida, você pode acessar o conteúdo da resposta
    data = response.json() # assume que a resposta é um JSON
else:
    # Se a resposta não foi bem sucedida, imprime uma mensagem de erro
    print("Erro ao realizar requisição. Status code: {}".format(response.status_code))
    
#Definindo estrutura que será usada pelo spark dataframe
schema = StructType([
    StructField("id", StringType(), nullable=True),
    StructField("name", StringType(), nullable=True),
    StructField("brewery_type", StringType(), nullable=True),
    StructField("address_1", StringType(), nullable=True),
    StructField("address_2", StringType(), nullable=True),
    StructField("address_3", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("state_province", StringType(), nullable=True),
    StructField("postal_code", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("longitude", StringType(), nullable=True),
    StructField("latitude", StringType(), nullable=True),
    StructField("phone", StringType(), nullable=True),
    StructField("website_url", StringType(), nullable=True),
    StructField("state", StringType(), nullable=True),
    StructField("street", StringType(), nullable=True)
])

df_raw = spark.createDataFrame(data, schema=schema)

#Escrevendo a raw
df_raw.write.mode("append").saveAsTable("bronze_brewery")

#Escrevendo a silver
df_silver = spark.sql("""
                      SELECT 
                            * 
                      FROM
                          bronze_brewery
                      """)

df_silver.write.format("parquet") \
        .partitionBy("city") \
        .mode("overwrite") \
        .saveAsTable("silver_brewery")

gold_brewery_df = silver_brewery_df \
    .groupBy("brewery_type", "city") \
    .count() \
    .withColumnRenamed("count", "tipos_cervejaria") \

gold_brewery_df.createOrReplaceTempView("gold_brewery")


# Definindo a DAG
dag = DAG(
    'executar_script_pyspark',
    description='Executar script PySpark',
    schedule_interval='0 0 * * *',  # Executar diariamente à meia-noite
    start_date=datetime(2023, 5, 18),
)

# Definindo o operador PythonOperator para executar o script PySpark
run_script_task = PythonOperator(
    task_id='executar_script',
    python_callable=run_pyspark_script,
    dag=dag,
)

# Definindo a ordem das tarefas na DAG
run_script_task
