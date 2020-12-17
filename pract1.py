# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 12:17:17 2020

@author: prach
"""

from pyspark import SparkConf,SparkContext
import collections

conf=SparkConf().setMaster('local').setAppName('Ratings')
sc=SparkContext(conf=conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/u.data")
rating = lines.map(lambda x:x.split()[2])

result =rating.countByValue()

sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))


