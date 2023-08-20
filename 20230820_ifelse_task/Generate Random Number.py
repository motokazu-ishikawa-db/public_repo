# Databricks notebook source
import random

random_int = random.randint( 1, 100 )
print(f"{random_int}を生成しました")
dbutils.jobs.taskValues.set( key = 'my_int', value = random_int )
dbutils.jobs.taskValues.set( key = 'is_odd', value = ( random_int % 2 == 1 ) )
