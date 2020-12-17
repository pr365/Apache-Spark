# -*- coding: utf-8 -*-
"""
Created on Thu Dec 17 13:33:06 2020

@author: prach
"""

from __future__ import print_function

from pyspark.ml.regression import DecisionTreeRegressor

from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler

if __name__ == "__main__":

    # Create a SparkSession (Note, the config section is only for Windows!)
    spark = SparkSession.builder.appName("DecisiontreeRegression").getOrCreate()

    # Load up our data and convert it to the format MLLib expects.
    data = spark.read.option("header", "true").option("inferSchema", "true")\
        .csv("file:///SparkCourse/realestate.csv")
    
    assembler = VectorAssembler().setInputCols(["HouseAge", "DistanceToMRT", \
                               "NumberConvenienceStores"]).setOutputCol('features')
    
    df = assembler.transform(data).select('PriceOfUnitArea','features')
    
    
    
    trainingtest=df.randomSplit([0.5,0.5])
    
    trainDF = trainingtest[0]
    testDF = trainingtest[1]
    
    dct = DecisionTreeRegressor().setFeaturesCol("features").setLabelCol("PriceOfUnitArea")
    
    model = dct.fit(trainDF)
    
    fullPredictions = model.transform(testDF).cache()
    
    predictions = fullPredictions.select("prediction").rdd.map(lambda x: x[0])
    labels = fullPredictions.select("PriceOfUnitArea").rdd.map(lambda x: x[0])

    # Zip them together
    predictionAndLabel = predictions.zip(labels).collect()

    # Print out the predicted and actual values for each point
    for prediction in predictionAndLabel:
      print(prediction)


    # Stop the session
    spark.stop()
