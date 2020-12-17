# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 16:08:01 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,StructType,StringType,IntegerType,FloatType

spark = SparkSession.builder.appName('mintemp').getOrCreate()

schema = StructType([\
                     StructField("stationID",StringType(),True), \
                     StructField("data",IntegerType(),True), \
                     StructField("measure_type",StringType(),True), \
                     StructField("temperature",FloatType(),True) ])

df=spark.read.schema(schema).csv("file:///SparkCourse/1800.csv")

df1=df.filter(df['measure_type']=='TMAX')

df1.groupBy('stationID').max('temperature').show()

spark.stop()



