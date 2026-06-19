import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

conn = snowflake.connector.connect(
    account=os.getenv('SNOWFLAKE_ACCOUNT'),
    user=os.getenv('SNOWFLAKE_USER'),
    password=os.getenv('SNOWFLAKE_PASSWORD'),
    database=os.getenv('SNOWFLAKE_DATABASE'),
    schema=os.getenv('SNOWFLAKE_SCHEMA'),
    warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
)

conn.cursor().execute("USE DATABASE FRAUD_DB")
conn.cursor().execute("USE SCHEMA FRAUD_SCHEMA")

conn.cursor().execute(
    "PUT file:///tmp/bank_fraud.csv @fraud_stage AUTO_COMPRESS=FALSE"
)

print("Upload complete")
conn.close()
