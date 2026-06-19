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

# USE DATABASE / USE SCHEMA are sent as separate execute() calls because
# Snowflake's Python connector runs one statement per call.
# We set them explicitly here even though they're in the connect() config above
# because PUT is a client-side command that needs the session context to be
# unambiguous before it resolves the stage name (@fraud_stage).
conn.cursor().execute("USE DATABASE FRAUD_DB")
conn.cursor().execute("USE SCHEMA FRAUD_SCHEMA")

# PUT uploads the local CSV to the Snowflake internal stage (@fraud_stage).
# The connector handles the actual file transfer — no manual S3/Azure upload needed.
# AUTO_COMPRESS=FALSE: the file is plain CSV; we don't want Snowflake to gzip it,
# because the COPY INTO command that follows expects an uncompressed file.
conn.cursor().execute(
    "PUT file:///tmp/bank_fraud.csv @fraud_stage AUTO_COMPRESS=FALSE"
)

print("Upload complete")
conn.close()
