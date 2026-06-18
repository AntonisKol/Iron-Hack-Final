from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, from_json
from pyspark.sql.types import StringType
from datetime import datetime
import snowflake.connector
import os
from utils import SNOWFLAKE_CONFIG, EVENT_SCHEMA as schema

BASE_DIR   = os.path.dirname(__file__)
CHECKPOINT = os.path.join(BASE_DIR, 'checkpoints', 's10_checkpoint')

# ── KEYWORD LISTS ─────────────────────────────────────────────────────────────
# match order: NEGATIVE checked first to avoid false positives from "not great"
POSITIVE_KEYWORDS = [
    'love', 'amazing', 'incredible', 'best', 'stunning', 'fantastic',
    'wonderful', 'inspiring', 'great', 'excellent', 'awesome', 'beautiful',
    'perfect', 'brilliant', 'superb',
]
NEGATIVE_KEYWORDS = [
    'terrible', 'worst', 'disappointed', 'horrible', 'awful', 'hate',
    'disgusting', 'pathetic', 'boring', 'useless', 'trash', 'waste',
    'bad', 'ugly', 'poor',
]


def classify_sentiment(text):
    if not text:
        return 'NEUTRAL'
    t = text.lower()
    for kw in NEGATIVE_KEYWORDS:
        if kw in t:
            return 'NEGATIVE'
    for kw in POSITIVE_KEYWORDS:
        if kw in t:
            return 'POSITIVE'
    return 'NEUTRAL'


sentiment_udf = udf(classify_sentiment, StringType())

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('S10 - Comment Sentiment Analysis') \
    .master('local[*]') \
    .config('spark.sql.shuffle.partitions', '4') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')
_log4j = spark.sparkContext._jvm.org.apache.log4j
_log4j.Logger.getLogger('org.apache.spark.sql.kafka010.KafkaDataConsumer').setLevel(_log4j.Level.ERROR)

# only COMMENT events have comment_text
raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', 'localhost:9092') \
    .option('subscribe', 'social-events') \
    .option('startingOffsets', 'earliest') \
    .option('failOnDataLoss', 'false') \
    .load()

comments_df = (
    raw_stream
    .select(from_json(col('value').cast('string'), schema).alias('d'))
    .select('d.*')
    .filter(col('event_type') == 'COMMENT')
    .filter(col('comment_text').isNotNull())
    .withColumn('sentiment', sentiment_udf(col('comment_text')))
)


def write_sentiment(batch_df, batch_id):
    if batch_df.isEmpty():
        return

    pdf     = batch_df.toPandas()
    conn    = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cur     = conn.cursor()
    now_str = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    rows    = []

    for _, row in pdf.iterrows():
        rows.append((
            str(row.get('event_id') or ''),
            str(row.get('user_id') or ''),
            str(row.get('post_id') or '') or None,
            str(row.get('comment_text') or ''),
            str(row.get('sentiment') or 'NEUTRAL'),
            str(row.get('timestamp') or now_str),
            now_str,
        ))

    cur.executemany(
        'INSERT INTO ANALYTICS.COMMENT_SENTIMENT '
        '(event_id, user_id, post_id, comment_text, sentiment, event_timestamp, processed_at) '
        'VALUES (%s, %s, %s, %s, %s, %s, %s)',
        rows,
    )
    conn.close()

    # quick distribution summary
    dist = pdf['sentiment'].value_counts().to_dict()
    print(f'  Batch {batch_id}: {len(rows)} comments → COMMENT_SENTIMENT  '
          f'[POS:{dist.get("POSITIVE",0)} NEU:{dist.get("NEUTRAL",0)} NEG:{dist.get("NEGATIVE",0)}]')


query = comments_df.writeStream \
    .foreachBatch(write_sentiment) \
    .trigger(processingTime='15 seconds') \
    .option('checkpointLocation', CHECKPOINT) \
    .start()

print('Sentiment analysis stream started. Consuming from Kafka topic: social-events\n')
query.awaitTermination()
