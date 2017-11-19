#!/usr/bin/env python
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, StopWordsRemover
from pyspark.ml.clustering import KMeans
from pyspark.ml import Pipeline

sc = SparkContext(appName="SparkClustering-emonto15-dperezg1")
spark = SparkSession(sc)

files = sc.wholeTextFiles("hdfs:///user/emonto15/datasets/Prueba1/")

schema =  StructType([StructField ("path" , StringType(), True) ,
StructField("text" , StringType(), True)])
df = spark.createDataFrame(files,schema)

tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
stopWords = StopWordsRemover(inputCol="tokens", outputCol="stopWordsRemovedTokens")
hashingTF = HashingTF(inputCol="stopWordsRemovedTokens", outputCol="rawFeatures", numFeatures=2000)
idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=1)
kmeans = KMeans(k=8)

#creacion del mapa de transformaciones
pipeline = Pipeline(stages=[tokenizer, stopWords, hashingTF, idf, kmeans])

#inserta el dataframe como el inicio de las transformaciones
model = pipeline.fit(df)

#ejecuta las trasformaciones mapeadas y guarda el resultado
results = model.transform(df)
results.cache()

#Imprime los Resultados
results.select("path","prediction").show()
