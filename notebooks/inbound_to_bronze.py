# Databricks notebook source
# MAGIC %md
# MAGIC #Conferindo se os dados foram montados e se o acesso está ok na pasta inbound

# COMMAND ----------

#verificando se o arquivo está montado e temos acesso.
dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

#criando a variável do arquivo
path = 'dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json'
#salvando na variavel dados
dados = spark.read.json(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC #Removendo colunas

# COMMAND ----------

dados_anuncio = dados.drop("imagens","usuario")
display(dados_anuncio)

# COMMAND ----------

# MAGIC %md
# MAGIC #Criando uma coluna de identificação

# COMMAND ----------

#importa a função col para criar uma nova coluna com o ID do anuncio
from pyspark.sql.functions import col



# COMMAND ----------

df_bronze = dados_anuncio.withColumn("id",col("anuncio.id"))
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC #Salvando na camada Bronze

# COMMAND ----------

path_bronze = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.mode(saveMode= "overwrite").format("delta").save(path_bronze)

# COMMAND ----------


