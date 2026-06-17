"""
Q19 — Sensor producer that simulates both on-time and late-arriving events.

WHAT THIS DOES:
  Phase 1 (on-time):  writes 60 readings with current timestamps.
                      Spark processes these and writes initial aggregates.
  Pause:              waits 35 seconds — enough for Spark to process Phase 1
                      and commit the window results to Snowflake.
  Phase 2 (late):     writes 15 readings with timestamps 7–9 minutes in the past.
                      These fall into windows Spark has ALREADY aggregated.
                      Spark re-emits those windows with updated counts.
                      The foreachBatch function detects the existing rows in
                      Snowflake and writes correction entries.

EXPECTED OUTPUT IN SNOWFLAKE:
  SENSOR_AGGREGATES:        rows updated (avg_temp and reading_count change)
  SENSOR_CORRECTIONS_HISTORY: original v1 rows + correction v2 rows side by side

This demonstrates the full late-data handling lifecycle.

Run AFTER starting q19_late_data_stream.py.
"""

import json
import os
import random
import time
from datetime import datetime, timedelta

INPUT_DIR = os.path.join(os.path.dirname(__file__), 'q19_sensor_input')
SENSORS   = ['S001', 'S002', 'S003']

print(f'Writing sensor events to: {INPUT_DIR}\n')


def write_event(sensor_id, temperature, ts, index, label):
    reading = {
        'sensor_id':   sensor_id,
        'temperature': temperature,
        'ts':          ts.strftime('%Y-%m-%dT%H:%M:%S'),
    }
    fname = os.path.join(INPUT_DIR, f'event_{index:04d}_{int(time.time() * 1000)}.json')
    with open(fname, 'w') as f:
        json.dump(reading, f)
    print(f'  {reading["ts"]}  {sensor_id}  {temperature:6.2f}°C  [{label}]')


# ── PHASE 1: on-time events ────────────────────────────────────────────────────
print('=== PHASE 1: on-time events (current timestamps) ===\n')

for i in range(60):
    sensor_id   = random.choice(SENSORS)
    temperature = round(random.uniform(18.0, 28.0), 2)
    ts          = datetime.utcnow()
    write_event(sensor_id, temperature, ts, i, 'on-time')
    time.sleep(0.2)

print('\n--- Waiting 35 seconds for Spark to process Phase 1 ---\n')
time.sleep(35)

# ── PHASE 2: late events (timestamps 7–9 minutes in the past) ─────────────────
print('=== PHASE 2: late events (timestamps 7-9 minutes ago) ===\n')
print('These fall into windows Spark already aggregated → corrections expected\n')

for i in range(15):
    sensor_id   = random.choice(SENSORS)
    temperature = round(random.uniform(18.0, 28.0), 2)
    # deliberately old timestamp — within the 10-minute watermark window
    minutes_late = random.uniform(7.0, 9.0)
    ts           = datetime.utcnow() - timedelta(minutes=minutes_late)
    write_event(sensor_id, temperature, ts, 60 + i, f'LATE {minutes_late:.1f}min')
    time.sleep(0.3)

print('\nAll events written.')
print('Check Snowflake: SENSOR_CORRECTIONS_HISTORY should show is_correction=TRUE rows.')
