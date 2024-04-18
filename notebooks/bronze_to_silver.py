# Databricks notebook source
dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC #Lendo os dados da camada Bronze

# COMMAND ----------

path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df = spark.read.format("delta").load(path)
display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC #Transformando os campos da coluna json em colunas

# COMMAND ----------

dados_detalhados =  df.select("anuncio.*", "anuncio.endereco.*")
df_silver = dados_detalhados.drop("caracteristicas","endereco")


# COMMAND ----------

display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC #Salvar na camada Silver

# COMMAND ----------

path_silver = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.mode(saveMode= "overwrite").format("delta").save(path_silver)
