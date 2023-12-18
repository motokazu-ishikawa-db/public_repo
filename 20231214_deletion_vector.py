# Databricks notebook source
# MAGIC %md
# MAGIC ## 削除ベクトルのテスト

# COMMAND ----------

# MAGIC %md
# MAGIC ### パラメーターの設定など準備

# COMMAND ----------

dbutils.widgets.text( "external_location_path", "s3://motokazu-ishikawa-demo/demo" )
dbutils.widgets.text( "catalog_name", "motokazu_ishikawa_demo" )
dbutils.widgets.text( "schema_name", "testschema" )
dbutils.widgets.text( "table_dv_false", "dv_false" )
dbutils.widgets.text( "table_dv_true", "dv_true" )

# COMMAND ----------

external_location_path = dbutils.widgets.get( "external_location_path" )
catalog_name = dbutils.widgets.get( "catalog_name" )
schema_name = dbutils.widgets.get( "schema_name" )
table_dv_false = dbutils.widgets.get( "table_dv_false" )
table_dv_true = dbutils.widgets.get( "table_dv_true" )

# COMMAND ----------

dbutils.fs.mkdirs(f"{external_location_path}")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS $catalog_name.$schema_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 削除ベクトル非適用のテーブルと、適用したテーブルを作成して初期データを投入します

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE $catalog_name.$schema_name.$table_dv_false (
# MAGIC   date INTEGER NOT NULL,
# MAGIC   delay INT NOT NULL,
# MAGIC   distance INT NOT NULL,
# MAGIC   origin STRING NOT NULL,
# MAGIC   destination STRING NOT NULL
# MAGIC )
# MAGIC LOCATION '$external_location_path/$table_dv_false'
# MAGIC TBLPROPERTIES ('delta.enableDeletionVectors' = false);

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE $catalog_name.$schema_name.$table_dv_true (
# MAGIC   date INTEGER NOT NULL,
# MAGIC   delay INT NOT NULL,
# MAGIC   distance INT NOT NULL,
# MAGIC   origin STRING NOT NULL,
# MAGIC   destination STRING NOT NULL
# MAGIC )
# MAGIC LOCATION '$external_location_path/$table_dv_true'
# MAGIC TBLPROPERTIES ('delta.enableDeletionVectors' = true);

# COMMAND ----------

df = ( spark.read.format("csv")
                 .option("header", True)
                 .option("inferSchema","true")
                 .load("dbfs:/databricks-datasets/flights/departuredelays.csv") )
df.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.{table_dv_false}")

# COMMAND ----------

df = ( spark.read.format("csv")
                 .option("header", True)
                 .option("inferSchema","true")
                 .load("dbfs:/databricks-datasets/flights/departuredelays.csv") )
df.write.mode("append").saveAsTable(f"{catalog_name}.{schema_name}.{table_dv_true}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 削除ベクトル非適用テーブルのDELETE前と後のファイル

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '$external_location_path/$table_dv_false'

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $catalog_name.$schema_name.$table_dv_false WHERE origin = 'ATL' AND destination = 'IAD';

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '$external_location_path/$table_dv_false'

# COMMAND ----------

# MAGIC %md
# MAGIC ### 削除ベクトル適用テーブルのDELETE前と後のファイル

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '$external_location_path/$table_dv_true'

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM $catalog_name.$schema_name.$table_dv_true WHERE origin = 'ATL' AND destination = 'IAD';

# COMMAND ----------

# MAGIC %sql
# MAGIC LIST '$external_location_path/$table_dv_true'
