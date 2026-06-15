import snowflake.connector

conn = snowflake.connector.connect(
    account='UOQWHPE-VQ18673',
    user='ANTONIS',
    password='Fourwheeldrive1#',
    database='FRAUD_DB',
    schema='FRAUD_SCHEMA',
    warehouse='COMPUTE_WH'
)

conn.cursor().execute("USE DATABASE FRAUD_DB")
conn.cursor().execute("USE SCHEMA FRAUD_SCHEMA")
conn.cursor().execute(
    "PUT file:///tmp/bank_fraud.csv @fraud_stage AUTO_COMPRESS=FALSE"
)

print("Upload complete")
conn.close()
