# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 19:18:58 2020

@author: prach
"""
from __future__ import print_function

from pyspark.ml.regression import LinearRegression

from pyspark.sql import SparkSession
from pyspark.ml.linalg import Vectors

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("LinearRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    inputLines = spark.sparkContext.textFile("regression.txt")
    rdd1 = inputLines.map(lambda x:x.split(',')).map(lambda x:(float(x[0]),Vectors.dense(float(x[1]))))
    
    columns =['label','features']
    df=rdd1.toDF(columns)
    
    trainingtest=df.randomSplit([0.5,0.5])
    
    trainDF = trainingtest[0]
    testDF = trainingtest[1]
    
    lir = LinearRegression(maxIter=10,regParam=0.3,elasticNetParam=0.8)
    model = lir.fit(trainDF)
    
    fullPredictions = model.transform(testDF).cache()
    
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("label").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Stop the session
    spark.stop()

                

