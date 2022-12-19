import pyspark
import pyspark.sql
from typing import Callable
from pyspark.sql.functions import decode
from pyspark.ml.feature import Tokenizer
import pyspark.sql.functions as f
import pyspark.sql.types as t
from pyspark.sql.functions import desc
from twitter.classes.filemanage import csvListFiles

conf = "spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0"

scala_version = '2.12'
spark_version = '3.3.0'

packages = [
    f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
    'org.apache.kafka:kafka-clients:3.3.0'
]

if __name__ == "__main__":

    csv_path = "/home/kiwichi/Documents/Projecte/data"
    files = csvListFiles(csv_path)

    sparktrans = pyspark.sql.SparkSession \
        .builder \
        .appName("SparkTwitter") \
        .config("spark.jars.packages", ",".join(packages))\
        .getOrCreate()
    sparktrans.sparkContext.setLogLevel("WARN")

    df = sparktrans.read \
        .option("header", "false") \
        .csv(files)

    my_udf = f.udf(lambda x: x.encode().decode('unicode-escape'),t.StringType())

    df2 = df.withColumn("_c1", my_udf('_c1'))

    tokenizer = Tokenizer(inputCol="_c1", outputCol="tokens")

    tokens = tokenizer.transform(df2)
    tokens.show(10)

    token_counts = tokens.select(f.explode("tokens").alias("token"))\
        .groupBy("token").count()\
        .orderBy(desc("count"))
    token_counts.show(truncate=False, n=20)

    token_counts.write \
    .mode("overwrite") \
    .csv("/home/kiwichi/Documents/Projecte/writtenData")