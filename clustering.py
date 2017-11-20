#!/usr/bin/env python
import sys
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, StopWordsRemover
from pyspark.ml.clustering import KMeans
if(len(sys.argv) > 5 ):
	sc = SparkContext(appName="SparkClustering-emonto15-dperezg1")
	spark = SparkSession(sc)
	files = sc.wholeTextFiles("hdfs://"+sys.argv[1])
	schema =  StructType([StructField ("path" , StringType(), True) ,StructField("text" , StringType(), True)])
	df = spark.createDataFrame(files,schema)

	tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
	StopWordsRemover.loadDefaultStopWords(sys.argv[4])
	stopWords = StopWordsRemover(inputCol="tokens", outputCol="stopWordsRemovedTokens")
	hashingTF = HashingTF(inputCol="stopWordsRemovedTokens", outputCol="rawFeatures", numFeatures=int(sys.argv[3]))
	idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=1)
	kmeans = KMeans(k=int(sys.argv[2]))
	
	#creacion del mapa de transformaciones
	pipeline = Pipeline(stages=[tokenizer, stopWords, hashingTF, idf, kmeans])
	
	#inserta el dataframe como el inicio de las transformaciones
	model = pipeline.fit(df)
	
	#ejecuta las trasformaciones mapeadas y guarda el resultado
	results = model.transform(df)
	results.cache()
	splited = split(results["path"], '.*/')
	results = results.withColumn("documents",splited.getItem(1))
	cluster = results.groupBy(['prediction']).agg(collect_list("documents").alias("cluster"))
	cluster.toJSON().coalesce(1).saveAsTextFile(sys.argv[5])
	print(cluster.select('prediction','cluster').toJSON().collect())
	#Imprime los Resultados
else:
	print("You are missing some arguments. Eg. clustering.py /datasets/gutenberg/19*.txt 4 2000 english")

