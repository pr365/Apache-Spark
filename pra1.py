# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 11:35:44 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark=SparkSession.builder.appName('SparkSQL').getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))
    
 
lines = spark.sparkContext.textFile("fakefriends.csv")
rdd = lines.map(mapper)

df = spark.createDataFrame(rdd).cache()
df.createOrReplaceTempView('friends')

teenagers = spark.sql('select * from friends where age>12 and age <20')

for teen in teenagers.collect():
  print(teen)
  
spark.stop()  

   