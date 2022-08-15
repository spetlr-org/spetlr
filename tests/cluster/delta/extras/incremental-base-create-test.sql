-- Databricks notebook source

CREATE DATABASE IF NOT EXISTS {IncrementalBaseDb}
COMMENT "Contains Incremental Base test data"
LOCATION "{IncrementalBaseDb_path}"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS {IncrementalBaseDummy}
(
    col1 INTEGER,
    col2 INTEGER,
    col3 STRING
)
USING DELTA
COMMENT "Contains Incremental Base test data"
LOCATION "{IncrementalBaseDummy_path}"

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS {IncrementalBaseDummy2}
(
    col1 INTEGER,
    col2 INTEGER,
    col3 STRING,
    timecol timestamp
)
USING DELTA
COMMENT "Contains Incremental Base test data"
LOCATION "{IncrementalBaseDummy2_path}"
