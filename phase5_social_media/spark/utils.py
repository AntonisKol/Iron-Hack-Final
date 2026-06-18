from pyspark.sql.types import StructType, StringType, FloatType, ArrayType
from dotenv import load_dotenv
import math
import os

load_dotenv('/Users/mpe/Desktop/Iron Hack/CAPSTONE /Final project/.env')

SNOWFLAKE_CONFIG = {
    'account':   os.getenv('SNOWFLAKE_ACCOUNT'),
    'user':      os.getenv('SNOWFLAKE_USER'),
    'password':  os.getenv('SNOWFLAKE_PASSWORD'),
    'database':  'SOCIAL_MEDIA_DB',
    'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE'),
}

EVENT_SCHEMA = StructType() \
    .add('event_id',           StringType()) \
    .add('event_type',         StringType()) \
    .add('user_id',            StringType()) \
    .add('post_id',            StringType()) \
    .add('target_user_id',     StringType()) \
    .add('hashtags',           ArrayType(StringType())) \
    .add('comment_text',       StringType()) \
    .add('content_type',       StringType()) \
    .add('video_duration_sec', FloatType()) \
    .add('watch_time_sec',     FloatType()) \
    .add('timestamp',          StringType())


def nan_to_none(v):
    if v is None:
        return None
    try:
        if math.isnan(float(v)):
            return None
    except (TypeError, ValueError):
        pass
    return v
