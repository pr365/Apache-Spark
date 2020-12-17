# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 13:22:56 2020

@author: prach
"""

from pyspark import SparkConf,SparkContext

conf=SparkConf().setMaster('local').setAppName('Agefriends')
sc=SparkContext(conf=conf)

def Parseline(line):
    fields=line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age,friends)


lines=sc.textFile("file:///SparkCourse/fakefriends.csv")

#rdd = lines.map(Parseline)

#rdd1 = rdd.groupByKey().mapValues(list)

#rdd2 = rdd1.mapValues(lambda x:sum(x)/len(x))

#results = rdd2.collect()

#for result in results:
#    print(result)
    
rdd=lines.map(Parseline)

rdd1=rdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1]))

rdd2=rdd1.mapValues(lambda x:x[0]/x[1])



results = rdd2.collect()

for result in results:
    print(result)



    

