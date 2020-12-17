# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 18:02:50 2020

@author: prach
"""

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster('local').setAppName('practice')

sc=SparkContext(conf=conf)



samples = sc.parallelize([
    ("abonsanto@fakemail.com", "Alberto", "Bonsanto"),
    ("mbonsanto@fakemail.com", "Miguel", "Bonsanto"),
    ("stranger@fakemail.com", "Stranger", "Weirdo"),
    ("dbonsanto@fakemail.com", "Dakota", "Bonsanto")
])


print (samples.collect())

samples.saveAsTextFile("file:///sparkcourse/here.txt")
read_rdd = sc.textFile("file:///sparkcourse/here.txt")

read_rdd.collect()



