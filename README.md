# spark-ttree

Implementation of ROOT I/O designed to get TTrees into Spark DataFrames.
Consists of the following three components:

* *DataSource* - Spark DataSourceV2 implementation
* *ArrayInterpretation* - Accepts raw TBasket byte ranges and returns deserialzed arrays
* *root_proxy* - Deserializes ROOT metadata to locate TBasket byte ranges
