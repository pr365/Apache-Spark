# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 18:17:48 2020

@author: prach
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, LongType
from pyspark.ml.recommendation import ALS
import sys
import codecs

def loadMovieNames():
    movieNames = {}
    # CHANGE THIS TO THE PATH TO YOUR u.ITEM FILE:
    with codecs.open("C:/SparkCourse/ml-100k/u.ITEM", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames


spark = SparkSession.builder.appName("ALSExample").getOrCreate()
    
moviesSchema = StructType([ \
                     StructField("userID", IntegerType(), True), \
                     StructField("movieID", IntegerType(), True), \
                     StructField("rating", IntegerType(), True), \
                     StructField("timestamp", LongType(), True)])
    
names = loadMovieNames()
    
ratings = spark.read.option("sep", "\t").schema(moviesSchema) \
    .csv("file:///SparkCourse/ml-100k/u.data")
    
print("Training recommendation model...")

als = ALS().setMaxIter(5).setRegParam(0.01).setUserCol('userID').setItemCol('movieID').setRatingCol('rating')

model = als.fit(ratings)

userID = int(sys.argv[1])
userschema = StructType([StructField('userid',IntegerType(),True)])
df = spark.createDataFrame([[userID,]],userschema)

recommendations = model.recommendForUserSubset(df,10).collect()


for i in recommendations:
    movierating = i[1]
    for j in movierating:
        moviename=names[j[0]]
        print(moviename+str(j[1]))
    

spark.stop()
