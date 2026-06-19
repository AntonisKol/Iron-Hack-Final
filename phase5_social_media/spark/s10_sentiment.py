# Step 10: Sentiment Analytics
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, udf
from pyspark.sql.types import StringType
from datetime import datetime
import snowflake.connector
import os
from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema

POSITIVE_WORDS = {'love', 'great', 'awesome', 'amazing', 'good', 'excellent', 'fantastic', 'wonderful', 'best', 'happy'}
NEGATIVE_WORDS = {'hate', 'bad', 'terrible', 'awful', 'worst', 'poor', 'horrible', 'disappointing', 'sad', 'angry'}

BASE_DIR = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's10_checkpoint')


def classify_sentiment(text):
    if not text:
        return 'neutral'
    words = set(text.lower().split())
    pos = len(words & POSITIVE_WORDS)
    neg = len(words & NEGATIVE_WORDS)
    if pos > neg:
        return 'positive'
    elif neg > pos:
        return 'negative'
    return 'neutral'

sentiment_udf = udf(classify_sentiment, StringType())

spark = SparkSession.builder \
    .appName('S10 - Sentiment Analytics') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
_log4j = spark.sparkContext._jvm.org.apache.log4j
_log4j.Logger.getLogger('org.apache.spark.sql.kafka010.KafkaDataConsumer').setLevel(_log4j.Level.ERROR)

spark.udf.register('classify_sentiment', classify_sentiment, StringType())

raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'social-events') \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false') \
    .load()

stream_df = raw_stream.select(
    from_json(col('value').cast('string'), schema).alias('d')
).select('d.*')

comment_df = (
    stream_df
    .filter(col('event_type') == 'COMMENT')
    .filter(col('comment_text').isNotNull())
    .withColumn('sentiment', sentiment_udf(col('comment_text')))
)


def write_sentiment(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf = batch_df.toPandas()
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

    for _, row in pdf.iterrows():
        cur.execute(
            'INSERT INTO ANALYTICS.COMMENT_SENTIMENT '
            '(event_id, user_id, post_id, comment_text, sentiment, event_timestamp, analyzed_at) '
            'VALUES (%s, %s, %s, %s, %s, %s, %s)',
            (
                str(row.get('event_id') or ''),
                str(row.get('user_id') or ''),
                str(row.get('post_id') or '') or None,
                str(row.get('comment_text') or ''),
                str(row.get('sentiment') or 'neutral'),
                str(row.get('timestamp') or now_str),
                now_str,
            ),
        )

    conn.close()
    print(f'  Batch {batch_id}: {len(pdf)} comments classified → COMMENT_SENTIMENT')


query = comment_df.writeStream \
    .foreachBatch(write_sentiment) \
    .trigger(processingTime='15 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Sentiment stream started. Consuming from Kafka topic: social-events\n')
query.awaitTermination()
