#!/usr/bin/env python
import sys
from time import time
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml import Pipeline
from pyspark.ml.feature import HashingTF, IDF, Tokenizer, CountVectorizer, StopWordsRemover
from pyspark.ml.clustering import KMeans
#Check if all the params were passed
if(len(sys.argv) > 5 ):
	#Setup the sparkContext
	sc = SparkContext(appName="SparkClustering-emonto15-dperezg1")
	spark = SparkSession(sc)
	t0 = time()
	#Read from hdfs and save using a schema (path,text)
	files = sc.wholeTextFiles("hdfs://"+sys.argv[1])
	schema =  StructType([StructField ("path" , StringType(), True) ,StructField("text" , StringType(), True)])
	df = spark.createDataFrame(files,schema)
	#Divide the text into an array of words
	tokenizer = Tokenizer(inputCol="text", outputCol="tokens")
	#Setup the language to remove the stopwords
	StopWordsRemover.loadDefaultStopWords(sys.argv[4])
	#Read from column tokens (which is the output of the tokenizer object) and save a new array of words without the stopwords
	stopWords = StopWordsRemover(inputCol="tokens", outputCol="stopWordsRemovedTokens")
	#Creates a hash of each word and the frecuency on each document and only takes the number of words established on the numFeatures parameter
	hashingTF = HashingTF(inputCol="stopWordsRemovedTokens", outputCol="rawFeatures", numFeatures=int(sys.argv[3]))
	#Calculates the inverse document frecuency, and ignore a word if  well explained on the code's article
	idf = IDF(inputCol="rawFeatures", outputCol="features", minDocFreq=1)
	#Initialize the kmeans with a specific K
	kmeans = KMeans(k=int(sys.argv[2]))
	#Declare the assambly line to transform the dataset
	#creacion del mapa de transformaciones
	pipeline = Pipeline(stages=[tokenizer, stopWords, hashingTF, idf, kmeans])
	#Apply the assambly line to the dataset
	model = pipeline.fit(df)
	#Execute each function to the dataset in order
	results = model.transform(df)
	t1 = time()
	#Cache keeps all the data on memory to improve the performance
	results.cache()
	#Split the path to get just the filename
	splited = split(results["path"], '.*/')
	results = results.withColumn("documents",splited.getItem(1))
	#Group by predictions and creates an array of all the documents that match that prediction
	cluster = results.groupBy(['prediction']).agg(collect_list("documents").alias("cluster"))
	#Save the Dataframe to a folder and overwrite if the data exist.
	cluster.coalesce(1).write.json(path=sys.argv[5], mode="overwrite")
	print(cluster.select('prediction','cluster').toJSON().collect())
else:
	print("You are missing some arguments. Eg. clustering.py /datasets/gutenberg/19*.txt 4 2000 english /tmp/out")
