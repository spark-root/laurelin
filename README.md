# spark-ttree [![Build Status](https://travis-ci.org/spark-root/laurelin.svg?branch=master)](https://travis-ci.org/spark-root/laurelin)[![Maven Central](https://img.shields.io/maven-central/v/edu.vanderbilt.accre/laurelin.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22edu.vanderbilt.accre%22%20AND%20a:%22laurelin%22)

Implementation of ROOT I/O designed to get TTrees into Spark DataFrames.
Consists of the following three components:

* *DataSource* - Spark DataSourceV2 implementation
* *ArrayInterpretation* - Accepts raw TBasket byte ranges and returns deserialzed arrays
* *root_proxy* - Deserializes ROOT metadata to locate TBasket byte ranges

The scope of this project is only to perform vectorized (i.e. column-based)
reads of TTrees consisting of relatively simple branches -- fundamental numeric
types and both fixed-length/jagged arrays of those types. More complex formats
like TTrees with branches containing C++ objects are explicitly not supported.

## Usage example

Laurelin users have a simple interface (developer docs are
[here](docs/CONTRIBUTING.md)). If you have the following:

* Apache Spark installed
* A ROOT file named `sample.root`
* A TTree within that file named `Events` 

Laurelin can load your TTree with the following PySpark snippet, without any
additional setup on your end:

```python
import pyspark.sql

spark = pyspark.sql.SparkSession.builder \
    .master("local[1]") \
    .config('spark.jars.packages', 'edu.vanderbilt.accre:laurelin:0.3.0') \
    .getOrCreate()
sc = spark.sparkContext
df = spark.read.format('root') \
                .option("tree", "Events") \
                .load('sample.root')
df.printSchema()
```

To use a different version, replace 0.3.0 with the desired version. 
The most recent version number is [![Maven Central](https://img.shields.io/maven-central/v/edu.vanderbilt.accre/laurelin?color=000000&label=%20&style=flat-square)](https://search.maven.org/search?q=g:%22edu.vanderbilt.accre%22%20AND%20a:%22laurelin%22)

## Known issues/not yet implemented functionality

* Float16/Doubles32 are currently not supported
* String types are currently not supported
