# Q14: Page View Streaming — Spark Structured Streaming
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, from_json
from pyspark.sql.types import StructType, StringType
import os

spark = SparkSession.builder \
    .appName('Q14 - Page View Stream') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

schema = StructType() \
    .add('user_id', StringType()) \
    .add('page', StringType()) \
    .add('timestamp', StringType())

raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'page-views') \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false') \
    .load()

stream_df = (
    raw_stream
    .select(from_json(col('value').cast('string'), schema).alias('d'))
    .select('d.*')
    .withColumn('ts', col('timestamp').cast('timestamp'))
)

windowed_counts = (
    stream_df
    .withWatermark('ts', '10 minutes')
    .groupBy(
        window(col('ts'), '5 minutes'),
        col('page')
    )
    .count()
)

query = windowed_counts.writeStream \
    .outputMode('update') \
    .format('console') \
    .option('truncate', False) \
    .trigger(processingTime='10 seconds') \
    .start()

print('\nStreaming started. Reading from Kafka topic: page-views')
print('Run q14_pageview_producer.py in another terminal.\n')

query.awaitTermination()
