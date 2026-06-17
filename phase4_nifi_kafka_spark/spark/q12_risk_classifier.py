from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from dotenv import load_dotenv
import snowflake.connector
import os

# load Snowflake credentials from .env
load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'database':  os.getenv('SNOWFLAKE_DATABASE'),
    'schema':    os.getenv('SNOWFLAKE_SCHEMA'),
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

JSON_PATH = os.path.join(os.path.dirname(__file__), 'risk_records.json')

# ── SPARK SESSION ─────────────────────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('Q12 - Risk Classifier') \
    .master('local[*]') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── STEP 1: READ JSON ─────────────────────────────────────────────────────────
# Spark reads JSON files in newline-delimited format (one JSON object per line).
# It infers the schema automatically — amount becomes LongType, credit_score LongType, etc.
# In production this could be: spark.read.json("s3://bucket/risk-events/")
df = spark.read.json(JSON_PATH)

print('=== Raw data ===')
df.show()
df.printSchema()

# ── STEP 2: DEFINE THE UDF ────────────────────────────────────────────────────
# This is a plain Python function — the risk classification rule set.
# Rules applied in order — first match wins:
#   HIGH   → amount > $10,000  OR  credit_score < 500  OR  failed_attempts > 3
#   MEDIUM → amount > $5,000   OR  credit_score < 650
#   LOW    → everything else
def classify_risk(amount, credit_score, failed_attempts):
    if amount > 10000 or credit_score < 500 or failed_attempts > 3:
        return 'high'
    elif amount > 5000 or credit_score < 650:
        return 'medium'
    else:
        return 'low'

# udf() wraps the Python function so Spark can call it on every row in parallel.
# StringType() tells Spark the return type — required so Spark can build its execution plan.
# Without registering as a UDF, Spark wouldn't know how to distribute the function.
classify_udf = udf(classify_risk, StringType())

# ── STEP 3: APPLY UDF ────────────────────────────────────────────────────────
# withColumn adds a new column 'risk_level' by calling the UDF on each row.
# The UDF receives three column values per row — still a TRANSFORMATION (lazy).
df_classified = df.withColumn(
    'risk_level',
    classify_udf(col('amount'), col('credit_score'), col('failed_attempts'))
)

print('=== Classified records ===')
df_classified.select('id', 'customer_id', 'amount', 'credit_score', 'failed_attempts', 'risk_level').show()

# ── STEP 4: SPLIT INTO THREE DATAFRAMES ──────────────────────────────────────
# Filter is a stateless transformation — each row is classified independently.
# .cache() tells Spark to keep df_classified in memory after first use,
# so the UDF doesn't re-run for each of the three filters below.
df_classified.cache()

df_high   = df_classified.filter(col('risk_level') == 'high')
df_medium = df_classified.filter(col('risk_level') == 'medium')
df_low    = df_classified.filter(col('risk_level') == 'low')

print(f'High risk:   {df_high.count()} records')
print(f'Medium risk: {df_medium.count()} records')
print(f'Low risk:    {df_low.count()} records')

# ── STEP 5: WRITE TO SNOWFLAKE ────────────────────────────────────────────────
# We use snowflake-connector-python here because spark-snowflake doesn't
# support PySpark 4.x yet. The logic is identical — toPandas() collects
# the Spark DataFrame to the driver, then we bulk-insert to Snowflake.
#
# In production with Spark 3.x + spark-snowflake connector, replace this
# entire function with df.write.format("net.snowflake.spark.snowflake")...

def write_to_snowflake(spark_df, table_name):
    """Convert Spark DataFrame to pandas and bulk-insert into Snowflake."""
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    # toPandas() is an ACTION — triggers Spark execution and collects to driver
    pandas_df = spark_df.toPandas()

    # drop the risk_level column — it's the table name, no need to repeat it
    pandas_df = pandas_df.drop(columns=['risk_level'])

    cols = ', '.join(pandas_df.columns)
    placeholders = ', '.join(['%s'] * len(pandas_df.columns))

    # CREATE OR REPLACE so the job is idempotent — safe to re-run
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            id             INTEGER,
            amount         FLOAT,
            country        VARCHAR,
            credit_score   INTEGER,
            customer_id    VARCHAR,
            failed_attempts INTEGER
        )
    """)

    # executemany inserts all rows in one batch — much faster than one INSERT per row
    rows = [tuple(row) for row in pandas_df.itertuples(index=False)]
    cursor.executemany(f'INSERT INTO {table_name} ({cols}) VALUES ({placeholders})', rows)

    print(f'  Written {len(rows)} rows → {table_name}')
    conn.close()

print('\n=== Writing to Snowflake ===')
write_to_snowflake(df_high,   'RISK_HIGH')
write_to_snowflake(df_medium, 'RISK_MEDIUM')
write_to_snowflake(df_low,    'RISK_LOW')

print('\nDone.')
spark.stop()
