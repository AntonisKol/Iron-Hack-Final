# Q11: Top Customers Per City — Spark Batch
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, col, rank, desc
from pyspark.sql.window import Window

# SPARK SESSION 
spark = SparkSession.builder \
    .appName('Q11 - Top Customers Per City') \
    .master('local[*]') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

# INLINE DATA 
# Two datasets created directly in code — no file I/O needed for the demo.
orders_data = [
    (1,  'C001', 250.00),
    (2,  'C002', 1800.00),
    (3,  'C001', 3200.00),
    (4,  'C003', 450.00),
    (5,  'C004', 12000.00),
    (6,  'C002', 900.00),
    (7,  'C005', 5500.00),
    (8,  'C003', 320.00),
    (9,  'C006', 7800.00),
    (10, 'C007', 150.00),
    (11, 'C004', 8000.00),
    (12, 'C008', 2200.00),
    (13, 'C005', 1100.00),
    (14, 'C009', 650.00),
    (15, 'C010', 9900.00),
    (16, 'C006', 3300.00),
    (17, 'C007', 4400.00),
    (18, 'C008', 1750.00),
    (19, 'C009', 5200.00),
    (20, 'C010', 2800.00),
]

customers_data = [
    ('C001', 'Alice',   'London'),
    ('C002', 'Bob',     'Paris'),
    ('C003', 'Charlie', 'London'),
    ('C004', 'Diana',   'Berlin'),
    ('C005', 'Eve',     'Paris'),
    ('C006', 'Frank',   'Berlin'),
    ('C007', 'Grace',   'London'),
    ('C008', 'Henry',   'Paris'),
    ('C009', 'Iris',    'Berlin'),
    ('C010', 'Jack',    'London'),
]

orders_df = spark.createDataFrame(orders_data, schema=['order_id', 'customer_id', 'amount'])
customers_df = spark.createDataFrame(customers_data, schema=['customer_id', 'name', 'city'])

print('=== Orders ===')
orders_df.show()

print('=== Customers ===')
customers_df.show()

# JOIN 
# Inner join: match each order to its customer.
# Spark builds an execution plan here but does NOT run it yet.
joined_df = orders_df.join(customers_df, on='customer_id', how='inner')

# AGGREGATE 
# Sum each customer's orders to get total_spend.
spend_df = joined_df.groupBy('customer_id', 'name', 'city') \
    .agg(_sum('amount').alias('total_spend'))

# WINDOW FUNCTION 
# partitionBy('city'): restart the ranking counter for each city independently.
# orderBy(desc('total_spend')): rank 1 = highest spender within that city.
window = Window.partitionBy('city').orderBy(desc('total_spend'))

ranked_df = spend_df.withColumn('rank', rank().over(window))

# FILTER TOP 5 
top5_df = ranked_df.filter(col('rank') <= 5) \
.orderBy('city', 'rank')

print('=== Top 5 customers by total spend per city ===')
top5_df.show()

# WRITE — PARTITIONED PARQUET 
# partitionBy('city'): output is split into subdirectories — one per city.
# Downstream readers can skip entire city folders they don't need (partition pruning).
# Spark executes the full plan here for the first time (lazy evaluation).
output_path = 'phase4_nifi_kafka_spark/spark/output/top_customers'

top5_df.write \
    .mode('overwrite') \
    .partitionBy('city') \
    .parquet(output_path)

print(f'Written to: {output_path}')
print('Partitions:')

import os
for root, dirs, files in os.walk(output_path):
    level = root.replace(output_path, '').count(os.sep)
    indent = '  ' * level
    folder = os.path.basename(root)
    if folder:
        print(f'{indent}{folder}/')
    for f in files:
        if f.endswith('.parquet'):
            print(f'{indent}  {f}')

spark.stop()
