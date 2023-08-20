# Databricks notebook source
random_int = dbutils.jobs.taskValues.get( taskKey = 'gen_random', key = 'my_int', )
print(f"{random_int}は奇数ですね!!!")
