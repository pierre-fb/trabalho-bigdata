// Databricks notebook source
// MAGIC %md
// MAGIC Transformação de arquivo CSV para o formato

// COMMAND ----------

 import org.apache.spark.sql.types._
 import spark.implicits._


// COMMAND ----------

val entradaArquivo = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/ListSum.parquet"
val bronzeDF = spark.read.parquet(entradaArquivo)

// COMMAND ----------

bronzeDF.printSchema


// COMMAND ----------


bronzeDF.select($"name",$"neighbourhood",$"price").show()

// COMMAND ----------

bronzeDF.createOrReplaceTempView("arquivoParquet")
val neighbourhoodDF = spark.sql("select neighbourhood, count(*) as count from arquivoParquet group by neighbourhood order by count desc")
neighbourhoodDF.show()

// COMMAND ----------

neighbourhoodDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/silver/neighCount.parquet")
