-- Databricks notebook source
-- MAGIC %python
-- MAGIC dbutils.fs.cp( "FileStore/tables/clinicaltrial_2021.csv", "/FileStore/tables/trial_table/")
-- MAGIC dbutils.fs.cp( "FileStore/tables/pharma.csv", "/FileStore/tables/pharm_table/")
-- MAGIC dbutils.fs.cp( "FileStore/tables/mesh.csv", "/FileStore/tables/mesh_table/")

-- COMMAND ----------

-- MAGIC %python
-- MAGIC import pyspark.sql.functions as f
-- MAGIC trial = spark.read.options(delimiter = "|", header = True, inferSchema = True).csv("/FileStore/tables/clinicaltrial_2021.csv")
-- MAGIC pharm = spark.read.options(escape = "\"", header = True, inferSchema = True).csv("/FileStore/tables/pharma.csv")
-- MAGIC mesh = spark.read.options(escape = "\"", header = True, inferSchema = True).csv("/FileStore/tables/mesh.csv").withColumn("code", f.split(f.col("tree"), "\.")[0]).withColumn("term", f.trim("term"))
-- MAGIC 
-- MAGIC trial.write.format("parquet").mode("overwrite").saveAsTable("trial")
-- MAGIC pharm.write.format("parquet").mode("overwrite").saveAsTable("pharm")
-- MAGIC mesh.write.format("parquet").mode("overwrite").saveAsTable("mesh")

-- COMMAND ----------

--PROBLEM 1
SELECT COUNT(DISTINCT(Id)) FROM trial
WHERE Id IS NOT NULL;

-- COMMAND ----------

--PROBLEM 2
SELECT Type, COUNT(*) freq
FROM trial
WHERE Type IS NOT NULL
GROUP BY Type
ORDER BY freq DESC;

-- COMMAND ----------

--PROBLEM 3
SELECT temp_cond, COUNT(*) freq
FROM (SELECT Conditions, EXPLODE(SPLIT(Conditions, ",")) temp_cond FROM trial
WHERE Conditions IS NOT NULL)
GROUP BY temp_cond
ORDER BY freq DESC;

-- COMMAND ----------

--PROBLEM 4
WITH c AS (
SELECT temp_cond, COUNT(*) freq
FROM (SELECT Conditions, EXPLODE(SPLIT(Conditions, ",")) temp_cond FROM trial
WHERE Conditions IS NOT NULL)
GROUP BY temp_cond)

SELECT m.code, SUM(c.freq) freq
FROM c
LEFT JOIN mesh m
ON c.temp_cond = m.term
GROUP BY m.code
ORDER BY freq DESC

-- COMMAND ----------

--PROBLEM 5
SELECT Sponsor, COUNT(*) freq
FROM trial
WHERE Sponsor NOT IN (SELECT DISTINCT(Parent_Company) FROM pharm)
GROUP BY Sponsor
ORDER BY freq DESC

-- COMMAND ----------

--PROBLEM 6
SELECT Completion, count(*) freq
FROM trial
WHERE Status == 'Completed' AND Completion LIKE '%2021'
GROUP BY Completion
ORDER BY freq desc;

-- COMMAND ----------


