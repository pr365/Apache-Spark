# -*- coding: utf-8 -*-
"""
Created on Mon Dec 14 16:13:29 2020

@author: prach
"""

import re
from pyspark import SparkConf, SparkContext

def normalizeWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

input = sc.textFile("file:///sparkcourse/book.txt")

rdd = input.flatMap(normalizeWords)


rdd1 = rdd.map(lambda x:(x,1)).reduceByKey(lambda x,y:x+y)


rdd2 = rdd1.map(lambda x:(x[1],x[0])).sortBykey()

rdd3=rdd2.sortBykey()

results = rdd3.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t\t" + count)


