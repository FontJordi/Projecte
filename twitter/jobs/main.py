import pyspark
import pyspark.sql
from typing import Callable

conf = "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

scala_version = '2.12'
spark_version = '3.3.0'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.0'
]

if __name__ == "__main__":
    
    spark = pyspark.sql.SparkSession \
        .builder \
        .appName("SparkTwitter") \
        .config("spark.jars.packages", ",".join(packages))\
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    print("hi")

    df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "Twitter") \
    .load()

    df2 = df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    
    writer = df2.writeStream \
        .format("csv") \
        .option("path", "/home/kiwichi/Documents/Projecte/data") \
        .option("checkpointLocation", "checkpoint") \
        .outputMode("append") \
        .trigger(processingTime="50 seconds") \
        .start()
    writer.awaitTermination()            
        #.option("checkpointLocation", "checkpoint") \
        #.option("path", "output_path/") \
        #.start() \
        #.awaitTermination(timeout=70)
        #.trigger(processingTime="60 seconds") \
##s'ha d'afegir una opcio de que si ja existeix un single csv file, appendd

#allfiles =  spark.read.option("header","false").csv("/home/kiwichi/Documents/Projecte/output_path/part-*.csv")    
#allfiles.coalesce(1).write.format("csv").option("header", "false").save("/home/kiwichi/Documents/Projecte/data/single_csv_file4/")