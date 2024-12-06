# Databricks notebook source
# MAGIC %md
# MAGIC #### **Criação do Banco de Dados**

# COMMAND ----------

#Criando o banco de dados
spark.sql("CREATE DATABASE IF NOT EXISTS db_clima_brasil")

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Atribuindo a chave privada do OpenWeather**

# COMMAND ----------

# Caminho para o arquivo no DBFS
file_path = "/FileStore/user_config/my_keys.txt"

# Usando Spark para ler o arquivo
df = spark.read.text(file_path)

# Acessando o valor do primeiro registro
api_key = df.head()[0]
#print(api_key)

# COMMAND ----------

# MAGIC %md
# MAGIC ### **Carregar lista de Cidades Brasileiras para extração**

# COMMAND ----------

# DBTITLE 1,Lista das cidades para Consulta
cidades_brasil = spark.read.csv('dbfs:/FileStore/Cidades_Brasil/Lista_de_10_cidades_Brasil.csv',   header=True, inferSchema=True)



# COMMAND ----------

# MAGIC %md
# MAGIC #### **Obter Dados Climáticos para as Cidades com a API do OpenWeather**

# COMMAND ----------

import requests
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from datetime import datetime
from pyspark.sql.types import StructType, StructField, DoubleType, StringType,TimestampType

schema = StructType([
    StructField("cidade", StringType(), nullable=True),
    StructField("estado", StringType(), nullable=True),
    StructField("id", StringType(), nullable=True),
    StructField("temperatura", StringType(), nullable=True),
    StructField("umidade", StringType(), nullable=True),
    StructField("pressao", StringType(), nullable=True),
    StructField("visibilidade", StringType(), nullable=True),
    StructField("velocidade_do_vento", StringType(), nullable=True),
    StructField("data", StringType(), nullable=True),
    StructField("descricao", StringType(), nullable=True),
    StructField("data_carga", TimestampType(), nullable=True),
    StructField("origem", StringType(), nullable=True)
])

def obter_dados_climaticos(cidade, estado, chave_api):
    """
    Função para obter dados climáticos de uma cidade utilizando a API do OpenWeather.
    
    :param cidade: Nome da cidade
    :param estado: Sigla do estado
    :param chave_api: Chave da API do OpenWeather
    :return: Dicionário com os dados climáticos
    """
     # Construindo a URL da API
    url = f"https://api.openweathermap.org/data/2.5/weather?q={cidade},{estado},BR&appid={api_key}"
    resposta = requests.get(url)
    
    if resposta.status_code == 200:
        dados = resposta.json()
        clima = {
            "cidade": cidade,
            "estado": estado,
            "id": dados["id"],
            "temperatura": dados["main"]["temp"],
            "umidade": dados["main"]["humidity"],
            "pressao": dados["main"]["pressure"],
            "visibilidade": dados["visibility"],
            "velocidade_do_vento": dados["wind"]["speed"],
            "data": datetime.utcfromtimestamp(dados['dt']).strftime('%Y-%m-%d %H:%M:%S'),
            "descricao": dados["weather"][0]["description"],
            "data_carga": datetime.now(),
            "origem": "OpenWeather"
        }
        return clima
    else:
        print(f"Erro ao acessar a API para {cidade} - {estado}")
        return None


# Inicializa a sessão Spark
spark = SparkSession.builder.appName("Pipeline_Clima_Brasil").getOrCreate()

# Lista para armazenar os dados
dados_climaticos = []

# Coleta os dados para todas as cidades
for cidade, uf,estado in cidades_brasil.collect():
    dados = obter_dados_climaticos(cidade, uf, api_key)
    if dados:
        dados_climaticos.append(dados)

# Cria DataFrame com os dados coletados
df_bronze = spark.createDataFrame(dados_climaticos,schema)


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Salvar os dados extraídos em tabela Bronze Delta**

# COMMAND ----------

# DBTITLE 1,Salvando os dados extraídos em raw parquet
# Salvar Bronze em Delta Table
caminho_bronze = "/mnt/delta/bronze/raw_data"
df_bronze.write.format("delta").mode("overwrite").save(caminho_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Criando a tabela Bronze Delta no Metastore**

# COMMAND ----------

# Criando a tabela Delta no Metastore
spark.sql("USE db_clima_brasil")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS clima_brasil_bronze
USING DELTA
LOCATION '{caminho_bronze}'
""")
