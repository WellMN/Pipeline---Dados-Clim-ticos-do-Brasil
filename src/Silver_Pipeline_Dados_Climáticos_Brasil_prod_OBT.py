# Databricks notebook source
# MAGIC %md
# MAGIC #### **Obtendo os dados da tabela Bronze**

# COMMAND ----------

# Obtendo os dados da Camada Bronze
df_clima_brasil_bronze = spark.sql(f"""
select * from db_clima_brasil.clima_brasil_bronze
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Tranformando os dados para ingresso na tabela Silver**

# COMMAND ----------

from pyspark.sql.functions import round,col, to_timestamp, date_format,current_timestamp


# Renomeando as colunas e ajustando os tipos
df_clima_brasil_silver = df_clima_brasil_bronze \
    .withColumnRenamed("estado", "uf") \
    .withColumnRenamed("cidade", "nome_da_cidade") \
    .withColumn("id", col("id").cast("int")) \
    .withColumn("temperatura", col("temperatura").cast("float")) \
    .withColumn("temperatura", round(col("temperatura"), 2)) \
    .withColumn("umidade", col("umidade").cast("int")) \
    .withColumn("pressao", col("pressao").cast("int")) \
    .withColumn("visibilidade", col("visibilidade").cast("int")) \
    .withColumn("velocidade_do_vento", col("velocidade_do_vento").cast("float")) \
    .withColumn("velocidade_do_vento", round(col("velocidade_do_vento"), 2)) \
    .withColumn("data", to_timestamp(col("data"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("data_carga_bronze", to_timestamp(col("data_carga"), "yyyy-MM-dd HH:mm:ss")) \
    .withColumn("data_transformacao_silver", current_timestamp()) 



# COMMAND ----------

# MAGIC %md
# MAGIC #### **Enriquecendo a tabela Silver com novos campos e removendo campos redundantes**

# COMMAND ----------

from pyspark.sql import functions as F

#Gerar Lista de Estados
ufs_estados = [
    ("AC", "ACRE"), ("AL", "ALAGOAS"), ("AM", "AMAZONAS"), ("AP", "AMAPÁ"), ("BA", "BAHIA"),
    ("CE", "CEARÁ"), ("DF", "DISTRITO FEDERAL"), ("ES", "ESPÍRITO SANTO"), ("GO", "GOIÁS"),
    ("MA", "MARANHÃO"), ("MT", "MATO GROSSO"), ("MS", "MATO GROSSO DO SUL"), ("MG", "MINAS GERAIS"),
    ("PA", "PARÁ"), ("PB", "PARAÍBA"), ("PE", "PERNAMBUCO"), ("PI", "PIAUÍ"), ("PR", "PARANÁ"),
    ("RJ", "RIO DE JANEIRO"), ("RN", "RIO GRANDE DO NORTE"), ("RS", "RIO GRANDE DO SUL"),
    ("RO", "RONDÔNIA"), ("RR", "RORAIMA"), ("SC", "SANTA CATARINA"), ("SP", "SÃO PAULO"),
    ("SE", "SERGIPE"), ("TO", "TOCANTINS")
]

# Criar um DataFrame com o mapeamento das UFs e seus respectivos estados
ufs_estados_df = spark.createDataFrame(ufs_estados, ["uf", "nome_estado"])

# Realizar o join entre o DataFrame original e o de mapeamento
df_clima_brasil_silver = df_clima_brasil_silver.join(
    ufs_estados_df, on="uf", how="left"
)
#Inclusão do País
df_clima_brasil_silver = df_clima_brasil_silver.withColumn(
    "pais", F.lit("BRASIL")
)

#Exclusão do campo data de carga que está duplicado
df_clima_brasil_silver = df_clima_brasil_silver.drop("data_carga")




# COMMAND ----------

# MAGIC %md
# MAGIC #### **Realizando a limpeza para dados inválidos**

# COMMAND ----------

from pyspark.sql import functions as F

# Remover linhas com valores nulos nas colunas 'nome_da_cidade', 'uf', 'data'
df_clima_brasil_silver_cleaned = df_clima_brasil_silver \
    .dropna(subset=["nome_da_cidade","id", "uf", "data"])

# Preencher os valores nulos nas demais colunas com 0 ou valores padrão apropriados
df_clima_brasil_silver_cleaned = df_clima_brasil_silver_cleaned \
    .fillna({
        "temperatura": 0.0,
        "umidade": 0,
        "pressao": 0,
        "visibilidade": 0,
        "velocidade_do_vento": 0.0,
        "descricao": "",
        "data_carga_bronze": "1970-01-01 00:00:00",  # Definindo como timestamp padrão
        "origem": ""
    })

#Atribuindo ao dafaframe da tabela Silver os dados corrigidos
df_clima_brasil_silver = df_clima_brasil_silver_cleaned

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Salvar a tabela Silver em Delta tale**

# COMMAND ----------

# Salvar o Dataframe df_clima_brasil_silver em Delta Table Silver
caminho_silver = "/mnt/delta/silver/clima_brasil_silver"
df_clima_brasil_silver.write.format("delta").mode("overwrite").save(caminho_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Criando a tabela Delta Silver no Metastore**

# COMMAND ----------

# Criando a tabela Delta Silver no Metastore
spark.sql("USE db_clima_brasil")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS clima_brasil_silver
USING DELTA
LOCATION '{caminho_silver}'
""")
