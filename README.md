# spark-ttree [![Build Status](https://travis-ci.org/PerilousApricot/spark-ttree.svg?branch=master)](https://travis-ci.org/PerilousApricot/spark-ttree)[![Maven Central](https://img.shields.io/maven-central/v/edu.vanderbilt.accre/laurelin.svg?label=Maven%20Central)](https://search.maven.org/search?q=g:%22edu.vanderbilt.accre%22%20AND%20a:%22laurelin%22)

Implementation of ROOT I/O designed to get TTrees into Spark DataFrames.
Consists of the following three components:

* *DataSource* - Spark DataSourceV2 implementation
* *ArrayInterpretation* - Accepts raw TBasket byte ranges and returns deserialzed arrays
* *root_proxy* - Deserializes ROOT metadata to locate TBasket byte ranges

The scope of this project is only to perform vectorized (i.e. column-based)
reads of TTrees consisting of relatively simple branches -- fundamental numeric
types and both fixed-length/jagged arrays of those types.

## Usage example

Note that the most recent version number can be found [here](https://search.maven.org/search?q=a:laurelin%20g:edu.vanderbilt.accre). To use a different version, replace 0.0.15 with your
desired version

```python
import pyspark.sql

spark = pyspark.sql.SparkSession.builder \
    .master("local[1]") \
    .config('spark.jars.packages', 'edu.vanderbilt.accre:laurelin:0.0.15') \
    .getOrCreate()
sc = spark.sparkContext
df = spark.read.format('root') \
                .option("tree", "tree") \
                .load('small-flat-tree.root')
df.printSchema()
```

## Known issues/not yet implemented functionality

* The I/O is currently completely unoptimized -- there is no caching or
  prefetching. Remote reads will be slow as a consequence.
* Arrays (both fixed and jagged) of booleans return the wrong result
* Float16/Doubles32 are currently not supported
* String types are currently not supported
* C++ STD types are currently not supported (importantly, std::vector)
