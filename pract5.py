# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 16:53:20 2020

@author: prach
"""

from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster('local').setAppName('customer')
sc=SparkContext(conf=conf)

def Parseline(line):
    fields = line.split(',')
    return (int(fields[0]), float(fields[2]))

input = sc.textFile("file:///sparkcourse/customer-orders.csv")
    
rdd = input.map(Parseline)

rdd1 = rdd.reduceByKey(lambda x,y:x+y)

rdd2 = rdd1.map(lambda x:(x[1],x[0]))

rdd3 = rdd2.sortByKey(False)

rdd4 = rdd3.map(lambda x:(x[1],x[0]))

results = rdd4.collect();
for result in results:
    print(result)