# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 16:36:02 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,StructType,IntegerType,FloatType

spark = SparkSession.builder.appName('customer').getOrCreate()

schema = StructType([ \
                     StructField("custid",IntegerType(),True),\
                     StructField("itemid",IntegerType(),True),\
                     StructField('price',FloatType(),True)]) 

df = spark.read.schema(schema).csv("file:///SparkCourse/customer-orders.csv")

df1=df.select(func.round(df['price'],0).alias('price1'))

df2 = df.select('custid')

df3=df2.join(df1)

df4=df3.groupBy('custid').sum('price1')

df4.show()


spark.stop()


