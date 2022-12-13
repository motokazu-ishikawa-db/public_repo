# Databricks notebook source
# MAGIC %pip install ja-ginza

# COMMAND ----------

# MAGIC %md
# MAGIC # Delta Live Tablesを使って某サッカー大会日本代表戦のツイートをリアルタイム分析

# COMMAND ----------

# Twitter APIから取得したJSONファイルを格納したディレクトリ
TWEET_DIR = "/user/motokazu.ishikawa@databricks.com/tweet/"
# 日本代表選手名をリストしたファイル
#　　1列目： name_alias
#　　２列目： name_canonical
# 抽出した人名がname_aliasに一致したらその選手に該当するものとする
PLAYER_DATA_CSV = "dbfs:/user/motokazu.ishikawa@databricks.com/japan_team_players.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC ## TwitterのFiltered Stream APIから取得した日本代表に関するツイートのJSONファイルがS3に保存されているのでAuto Loaderを使ってDLTに読み込みます

# COMMAND ----------

import dlt
from pyspark.sql.types import *
from pyspark.sql.functions import current_timestamp, explode, get_json_object, split, udf, col
from pyspark.sql import functions as f

@dlt.table
@dlt.expect_or_drop("Valid_tweet", "text IS NOT NULL")
def tweet():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "json")
      .load(TWEET_DIR)
      .withColumn( "text", get_json_object( "data", "$.text").alias("text") )
      .withColumn( "id", get_json_object( "data", "$.id").alias("id") )
      .withColumn( "timestamp", current_timestamp() )
      .drop("data")
  )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 日本代表選手の選手名一覧のファイルを読み込みます

# COMMAND ----------

@dlt.table
def player_name():
  return ( spark
          .read
          .format("csv")
          .option("header","true")
          .load(PLAYER_DATA_CSV) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ツイートからPersonの固有表現抽出を抽出

# COMMAND ----------

from pyspark.sql.functions import length
import spacy
nlp = spacy.load("ja_ginza")

@udf
def get_ner(s):
  for ent in nlp(s).ents:
    if ent.label_ == 'Person':
      return ent.text
  return ""
  
@dlt.table
def tweeted_person():
  return ( dlt.read_stream("tweet")
          .withColumn( "person", get_ner( f.col("text") ) )
          .filter( length("person") != 0 ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## 日本選手名に該当するものを抽出

# COMMAND ----------

@dlt.table
def tweeted_player():
  df_tweeted_person = dlt.read_stream("tweeted_person").drop("matching_rules")
  df_player_name = dlt.read("player_name")
  return ( df_tweeted_person
          .join( df_player_name, df_tweeted_person.person == df_player_name.name_alias )
          .drop( "text", "name_alias" ) )

# COMMAND ----------

# MAGIC %md
# MAGIC ## ツイート数を選手ごとに集計（ウィンドウ5分）

# COMMAND ----------

from pyspark.sql.functions import window
@dlt.table
def player_count():
  return ( dlt.read_stream("tweeted_player")
          .withWatermark("timestamp", "10 minutes")
          .groupBy( window( "timestamp", "5 minutes", "2 minutes"), "name_canonical" )
          .count()) 
