# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 15:38:09 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Wordcount").getOrCreate()

df=spark.read.text("file:///SparkCourse/book.txt")

df1 = df.select(func.explode(func.split(df.value,'\\W+')).alias('word'))

df2 = df1.filter(df1['word']!=' ')

df3=df2.groupBy('word').count()

df3.sort('count',ascending=False).show()

