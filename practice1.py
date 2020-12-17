# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 17:31:04 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName('practice').getOrCreate()

def mapper(line):
    fields=line.split(',')
    return (int(fields[0]), float(fields[2]))


lines = spark.sparkContext.textFile("file:///SparkCourse/customer-orders.csv")
rdd =lines.map(mapper)

df=spark.createDataFrame(rdd,['custid','price'])

df.printSchema()

df.show()
