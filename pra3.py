# -*- coding: utf-8 -*-
"""
Created on Tue Dec 15 12:26:24 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName('joining').getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), \
               age=int(fields[2]), numFriends=int(fields[3]))
    
lines = spark.sparkContext.textFile("fakefriends.csv")
rdd   = lines.map(mapper)

df=spark.createDataFrame(rdd).cache()
df.createOrReplaceTempView('friends')

df1=spark.read.option('header','true').option('inferschema','true').csv("file:///SparkCourse/fakefriends-header.csv")
df1.createOrReplaceTempView('mitron')

samefriends = spark.sql('select distinct f.ID,f.numFriends from friends f,mitron m where f.ID>m.userID and f.numFriends=m.friends')

for teen in samefriends.collect():
  print(teen)

spark.stop()