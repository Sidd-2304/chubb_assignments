-- Databricks notebook source
USE CATALOG siddarthasamisetty;
USE dlt_nyc_taxi_db;
SHOW TABLES;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Check Bronze layer sample

-- COMMAND ----------

SELECT *
FROM taxi_raw_records
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Validate expectation rule worked (distance > 0)

-- COMMAND ----------

SELECT COUNT(*) AS invalid_trip_distance_rows
FROM taxi_raw_records
WHERE trip_distance <= 0;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC View suspicious flagged rides

-- COMMAND ----------

SELECT *
FROM flagged_rides
ORDER BY fare_amount DESC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Review weekly aggregated results

-- COMMAND ----------

SELECT *
FROM weekly_stats
ORDER BY week ASC
LIMIT 10;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC Validate Gold output (top N highest rides)

-- COMMAND ----------

SELECT *
FROM top_n
ORDER BY fare_amount DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Additional business-level insight check

-- COMMAND ----------

SELECT week, COUNT(*) AS flagged_count
FROM flagged_rides
GROUP BY week
ORDER BY week;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC

-- COMMAND ----------

