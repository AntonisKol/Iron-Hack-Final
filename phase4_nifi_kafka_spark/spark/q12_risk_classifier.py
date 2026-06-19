# Q12: UDF Risk Classifier — Spark Batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StringType
from dotenv import load_dotenv
import snowflake.connector
import os

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

spark = SparkSession.builder \
    .appName('Q12 - Risk Classifier') \
    .master('local[*]') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# ── LOAD DATA ─────────────────────────────────────────────────────────────────
df = spark.read.json(JSON_PATH)

print('=== Raw data ===')
df.show()
df.printSchema()

# ── UDF DEFINITION ────────────────────────────────────────────────────────────
# A regular Python function that applies the business rule.
# Three inputs, one string output — a label per record.
def classify_risk(amount, credit_score, failed_attempts):
    if amount > 10000 or credit_score < 500 or failed_attempts > 3:
        return 'high'
    elif amount > 5000 or credit_score < 650:
        return 'medium'
    else:
        return 'low'

# ── UDF REGISTRATION ──────────────────────────────────────────────────────────
# udf() wraps the Python function so Spark can call it on each row in parallel.
# StringType() tells Spark what the return type is — required at registration time.
classify_udf = udf(classify_risk, StringType())

# ── APPLY UDF ────────────────────────────────────────────────────────────────
# withColumn adds a new column; classify_udf is called once per row, across all cores.
df_classified = df.withColumn(
    'risk_level',
    classify_udf(col('amount'), col('credit_score'), col('failed_attempts'))
)

print('=== Classified records ===')
df_classified.select('id', 'customer_id', 'amount', 'credit_score', 'failed_attempts', 'risk_level').show()

# ── CACHE ─────────────────────────────────────────────────────────────────────
# cache() stores the classified DataFrame in memory after the first action.
# Without it, the three .count() calls below would each re-run the UDF from scratch.
df_classified.cache()

# ── SPLIT BY RISK LEVEL ───────────────────────────────────────────────────────
df_high   = df_classified.filter(col('risk_level') == 'high')
df_medium = df_classified.filter(col('risk_level') == 'medium')
df_low    = df_classified.filter(col('risk_level') == 'low')

print(f'High risk:   {df_high.count()} records')
print(f'Medium risk: {df_medium.count()} records')
print(f'Low risk:    {df_low.count()} records')


# ── WRITE TO SNOWFLAKE ────────────────────────────────────────────────────────
# toPandas() converts a Spark DataFrame to a pandas DataFrame for row-level inserts.
# risk_level is dropped before writing — each table already represents one level.
def write_to_snowflake(spark_df, table_name):
    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()

    pandas_df = spark_df.toPandas()
    pandas_df = pandas_df.drop(columns=['risk_level'])

    cols = ', '.join(pandas_df.columns)
    placeholders = ', '.join(['%s'] * len(pandas_df.columns))

    # CREATE OR REPLACE makes this idempotent — safe to re-run without duplicates
    cursor.execute(f"""
        CREATE OR REPLACE TABLE {table_name} (
            id              INTEGER,
            amount          FLOAT,
            country         VARCHAR,
            credit_score    INTEGER,
            customer_id     VARCHAR,
            failed_attempts INTEGER
        )
    """)

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
