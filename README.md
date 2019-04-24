# spark-ttree [![Build Status](https://travis-ci.org/PerilousApricot/spark-ttree.svg?branch=master)](https://travis-ci.org/PerilousApricot/spark-ttree)

Implementation of ROOT I/O designed to get TTrees into Spark DataFrames.
Consists of the following three components:

* *DataSource* - Spark DataSourceV2 implementation
* *ArrayInterpretation* - Accepts raw TBasket byte ranges and returns deserialzed arrays
* *root_proxy* - Deserializes ROOT metadata to locate TBasket byte ranges

The scope of this project is only to perform vectorized (i.e. column-based)
reads of TTrees consisting of relatively simple branches -- fundamental numeric
types and both fixed-length/jagged arrays of those types.
