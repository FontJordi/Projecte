##bon dia

import pyspark
import pyspark.sql
from typing import Callable

conf = "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1"

if __name__ == "__main__":
    
    spark = pyspark.sql.SparkSession \
        .builder \
        .appName("Spark stream twitter data") \
        .config(conf = conf) \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")


    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .load()
    
    