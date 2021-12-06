// Databricks notebook source


// COMMAND ----------



// COMMAND ----------



// COMMAND ----------

val lines = sc.textFile("dudh.txt")
val words = lines.flatMap(line => line.split(" "))
val wordsFilt = words.filter(x => x.length > 0)
val counts = wordsFilt.map(word => (word.toUpperCase,1))
val countsReduced = counts.reduceByKey((v1, v2) => v1 + v2)
countsReduced.take(10)
