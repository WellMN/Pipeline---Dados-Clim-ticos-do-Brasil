# Databricks notebook source
# MAGIC %md
# MAGIC #### **Obtendo os dados da Camada Silver**

# COMMAND ----------

#Obter dados da Camada Silver
df_clima_brasil_silver = spark.sql(f"""
select * from db_clima_brasil.clima_brasil_silver
""")

df_clima_brasil_silver.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC #### **Criando a tabela wide na Camada Gold**

# COMMAND ----------

# Salvar a tabela como Delta na camada gold
caminho_gold = "/mnt/delta/gold/clima_brasil_gold"
df_clima_brasil_silver.write.format("delta").mode("overwrite").save(caminho_gold)

#Registrar a tabela fato_clima_brasil_gold no Metastore
spark.sql("USE db_clima_brasil")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS clima_brasil_gold
USING DELTA
LOCATION '{caminho_gold}'
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #### **Criando a tabela agregada na camada gold**

# COMMAND ----------

from pyspark.sql import functions as F

# Realizar a agregação por estado
df_agg_clima_brasil_gold = df_clima_brasil_silver.groupBy("nome_estado") \
    .agg(
        F.round(F.avg("temperatura"), 2).alias("media_temperatura"),
        F.round(F.avg("umidade"), 2).alias("media_umidade"),
        F.round(F.avg("pressao"), 2).alias("media_pressao"),
        F.round(F.avg("velocidade_do_vento"), 2).alias("media_velocidade_do_vento"),
        F.min("data").alias("data_primeira_leitura"),
        F.max("data").alias("data_ultima_leitura"),
        F.countDistinct("nome_da_cidade").alias("qtd_cidades")
    )


# Adicionar coluna para registrar a data de transformação na camada gold
df_agg_clima_brasil_gold = df_agg_clima_brasil_gold.withColumn("data_transformacao_gold", F.current_timestamp())

# Definir o caminho da tabela Delta
caminho_gold = "/mnt/delta/gold/agg_clima_brasil_gold"
df_agg_clima_brasil_gold.write.format("delta").mode("overwrite").save(caminho_gold)

df_agg_clima_brasil_gold.show(2)

#Registrar a tabela agg_clima_brasil_gold no Metastore
spark.sql("USE db_clima_brasil")

spark.sql(f"""
CREATE TABLE IF NOT EXISTS agg_clima_brasil_gold
USING DELTA
LOCATION '{caminho_gold}'
""")


# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select * from agg_clima_brasil_gold
