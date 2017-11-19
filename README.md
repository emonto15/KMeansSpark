# Big Data K-Means Clustering

Is an implementation of K-means algorithm in PySpark (Python And Apache Spark).

## Getting Started

There is only one Python file that contains all the code of the k-means implementation.

This project was build for running over a Hadoop Cluster, with PySpark, Yarn, Hive and other tools from the Apache Hadoop Ecosystem.

For Testing and Development of the code we based on a hortonworks cluster; Which is based on the Apache Hadoop Project.

Testing PySpark:
```
$ pyspark --version

SPARK_MAJOR_VERSION is set to 2, using Spark2
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 2.1.1.2.6.1.0-129
      /_/

Using Scala version 2.11.8, Java HotSpot(TM) 64-Bit Server VM, 1.8.0_144
Branch HEAD
Compiled by user jenkins on 2017-05-31T03:30:24Z
Revision e6dcaa0cd2f08f003ac34af03097c2f41209f065
Url git@github.com:hortonworks/spark2.git
Type --help for more information.

```

### Prerequisites

- [hortonworks Cluster](https://hortonworks.com/) ->  2.6.3
- Python -> 3.6
- Spark -> 2.1.1
- Scala -> 2.11.8
- Java JRE -> 1.8.0_144

When installing hortonworks project, it covers other prerequisites such as HDFS, Ambari, etc.

### Usage

The code receive four parameters to work this are:
1. The HDFS path to the documents. (It require just the first slash for absolute path. The HDFS:// is already set)
2. The number of documents clusters you want (For the K-Means).
3. The maximum number of features (words) you want to limit the documents.
4. The language of the documents (This is because we remove the stopwords of the language).The languages supported are: danish, dutch, english, finnish, french, german, hungarian, italian, norwegian, portuguese, russian, spanish, swedish, turkish.

There are two ways to run the code:


1. Locally on the master of the cluster:

```
$spark-submit --master local --deploy-mode client clustering.py /datasets/gutenberg-txt-es/1*.txt 8 2000 spanish
```
This example's dataset is located at /datasets/gutenberg-txt-es/ and only will use all the documents that start with 1 and are text files, it use 8 clusters for the k-means, also limit the documents to 2000 different features, the language of the documents is spanish and the output file is output.txt

2. Using all the Hadoop Cluster:
```
$spark-submit --master yarn --deploy-mode cluster  --executor-memory 2G --num-executors 4 clustering.py /datasets/gutenberg/19*.txt 4 2000 english
```
This example's dataset is located at /datasets/gutenberg/ and only will use all the documents that start with 19 and are text files, it use 4 clusters for the k-means, also limit the documents to 2000 different features and the language of the documents is english

The master and deploy-mode flags are from the spark-submit executable, for further reading please read the [Full Documentation](https://spark.apache.org/docs/2.1.1/submitting-applications.html)

## Authors

* **Diego Alejandro Perez**
* **Edwin Montoya Jaramillo**

### Testing on a server (DCA):
The Department of computer science of EAFIT University has a big data cluster, based on a master and two slaves.
```
$ ssh <VPN Username>@192.168.10.75 #Cluster's Master
>password:*********
<VPN Username>@hdplabmaster:~/$git clone
<VPN Username>@hdplabmaster:~/$cd bigDataK-means/
<VPN Username>@hdplabmaster:~/$spark-submit --master yarn --deploy-mode cluster  --executor-memory 2G --num-executors 4 clustering.py /datasets/gutenberg/19*.txt 4 2000 english
<VPN Username>@hpcdis:~/$

```
## Acknowledgments

* Edwin Nelson Montoya Múnera
* Juan David Pineda Cardenas
* Juan Francisco Cardona Mc'Cormick
* Daniel Hoyos Ospina
* Daniela Serna Escobar
* Daniel Rendon