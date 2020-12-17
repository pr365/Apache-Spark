# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 11:59:46 2020

@author: prach
"""

from pyspark.sql import SparkSession

spark=SparkSession.builder.appName('sparkdf').getOrCreate()

df=spark.read.option("header","true").option("inferSchema","true").csv("file:///SparkCourse/fakefriends-header.csv")

df.printSchema()

df.select(df.age).show()

df.filter(df.friends>200).show()

spark.stop() 