# Databricks notebook source
# MAGIC %md
# MAGIC # SparkでExcelファイルの入出力をする
# MAGIC 
# MAGIC ## [spark-excel](https://github.com/crealytics/spark-excel)ライブラリを使います
# MAGIC 
# MAGIC ## インストール方法
# MAGIC クラスターのライブラリ設定でMavenのcom.crealytics:spark-excel_2.12:3.3.1_0.18.5 を設定してインストールしてください。

# COMMAND ----------

# MAGIC %md
# MAGIC ### 変数設定
# MAGIC 以下のファイルパスにExcelファイルを出力します。必要に応じて適宜変更ください。

# COMMAND ----------

xlsx_file_path = "dbfs:/tmp/sample-excel.xlsx"

# COMMAND ----------

# MAGIC %md
# MAGIC ### SparkデータフレームをExcelファイルとして出力する
# MAGIC [Databricksのサンプルデータセット](https://qiita.com/taka_yayoi/items/3f8ccad13c6efd242be1)の、フライト情報の先頭20行を試しにExcelファイルとして出力してみます

# COMMAND ----------

df_output = ( spark
       .read
       .option( "header","true" )
       .csv( "dbfs:/databricks-datasets/flights/departuredelays.csv" )
       .limit(20) )

# COMMAND ----------

( df_output.write
  .format( "com.crealytics.spark.excel" )
  .option( "header", "true" )
  .mode( "overwrite" )
  .save( xlsx_file_path ) )

# COMMAND ----------

dbutils.fs.ls( xlsx_file_path )

# COMMAND ----------

# MAGIC %md
# MAGIC ### ExcelファイルをSparkデータフレームとして読み込む
# MAGIC 上で出力したExcelファイルを読み込み、中身を確認してみます

# COMMAND ----------

df_input = ( spark.read.format( "com.crealytics.spark.excel" )
    .option( "header", "true" )
    .option("dataAddress", "Sheet1")
    .option( "inferSchema", "false" )
    .load( xlsx_file_path ) )

# COMMAND ----------

display( df_input )

# COMMAND ----------

# MAGIC %md
# MAGIC ### 既存のExcelファイルにSparkデータフレームのデータを追加する

# COMMAND ----------

df_flight = spark.createDataFrame( [["HND","SFO"],["HND","CDG"],["CDG","HND"]] )
( df_flight.write
  .format( "com.crealytics.spark.excel" )
  .option( "dataAddress", "'Sheet1'!D5" )
  .option( "header", "false" )
  .mode( "append" )
  .save( xlsx_file_path ) )
