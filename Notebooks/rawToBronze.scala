// Databricks notebook source
import org.apache.spark.sql.types._
import spark.implicits._

// COMMAND ----------

val inArqRevSum = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/raw/reviews_summary.csv"
val rawRevSumDF = spark.read.format("csv").option("header","true").load(inArqRevSum)

val inArqRev = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/raw/reviews.csv"
val rawRevDF = spark.read.format("csv").option("header","true").load(inArqRev)

val inArqCal = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/raw/calendar.csv"
val rawCalDF = spark.read.format("csv").option("header","true").load(inArqCal)

val inArqList = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/raw/listings.csv"
val rawListDF = spark.read.format("csv").option("header","true").load(inArqList)

val inArqListSum = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/raw/listings_summary.csv"
val rawListSumDF = spark.read.format("csv").option("header","true").load(inArqListSum)

val inArqNeigh = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/raw/neighbourhoods.csv"
val rawListNeighDF = spark.read.format("csv").option("header","true").load(inArqNeigh)



// COMMAND ----------

val inArqNeighGeo = "abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/raw/neighbourhoods.geojson"
val rawNeighGeoDF = spark.read.format("json").option("multiline",true).load(inArqNeighGeo)


// COMMAND ----------

rawRevSumDF.printSchema
rawRevSumDF.show(5)


// COMMAND ----------

rawRevDF.show(5)
rawRevDF.printSchema


// COMMAND ----------

rawCalDF.printSchema
rawCalDF.show(5)


// COMMAND ----------

rawListDF.printSchema
rawListDF.show(5)



// COMMAND ----------

rawListSumDF.printSchema
rawListSumDF.show(5)


// COMMAND ----------

rawListNeighDF.printSchema
rawListNeighDF.show(5)


// COMMAND ----------

rawNeighGeoDF.printSchema
rawNeighGeoDF.show(5)

// COMMAND ----------

val bronzeListSumDF = rawListSumDF.count()

// COMMAND ----------

val bronzeListNeighFilterDF = rawListNeighDF.select($"neighbourhood")

// COMMAND ----------

val bronzeNeighGeoDF = rawNeighGeoDF.filter($"features.properties.neighbourhood".isNotNull)

// COMMAND ----------

val bronzeListDF = rawListDF.filter($"id".isNotNull)

// COMMAND ----------

rawRevSumDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/RevSum.parquet")

// COMMAND ----------

rawRevDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/Rev.parquet")

// COMMAND ----------

rawCalDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/Cal.parquet")

// COMMAND ----------

bronzeListDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/List.parquet")

// COMMAND ----------

rawListSumDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/ListSum.parquet")

// COMMAND ----------

bronzeListNeighFilterDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/ListNeigh.parquet")

// COMMAND ----------

bronzeNeighGeoDF.write.mode("overwrite").parquet("abfss://dados@datalaketrab0001.dfs.core.windows.net/dados/bronze/neighGeo.parquet")
