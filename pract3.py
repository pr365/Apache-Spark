# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 15:45:04 2020

@author: prach
"""

from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster('local').setAppName('mintemp')
sc=SparkContext(conf=conf)

def Parseline(line):
    fields = line.split(',')
    stationid = fields[0]
    entrytype = fields[2]
    temperature = fields[3]
    return (stationid,entrytype,temperature)


lines = sc.textFile("file:///SparkCourse/1800.csv")
Parsedlines = lines.map(Parseline)

rdd = Parsedlines.filter(lambda x:'TMIN' in x[1])

rdd1 = rdd.map(lambda x:(x[0],x[2]))

rdd2 = rdd1.reduceByKey(lambda x,y:min(x,y))

results = rdd2.collect()

for result in results:
    print(result)