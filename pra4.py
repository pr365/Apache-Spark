# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 13:41:58 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark=SparkSession.builder.appName('friendsbyage').getOrCreate()

df = spark.read.option("header","true").option('inferSchema','true').csv("file:///SparkCourse/fakefriends-header.csv")

df1 = df.agg(func.min('friends')).first()

df1.show()

spark.stop()

