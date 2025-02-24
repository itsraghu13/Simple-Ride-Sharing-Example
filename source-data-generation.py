# Databricks notebook source

# pip install faker
# pip install confluent-kafka

# COMMAND ----------

import json
import random
import time
from faker import Faker
from confluent_kafka import Producer
from datetime import datetime, timedelta

# COMMAND ----------

fake = Faker()

# COMMAND ----------

def generate_ride_event():
    ride_id = fake.uuid4()
    user_id = fake.uuid4()
    driver_id = fake.uuid4()
    pickup_location = fake.address()
    drop_location = fake.address()
    fare = round(random.uniform(5, 50), 2)
    distance_km = round(random.uniform(1, 30), 2)

    # Get today's date and generate a random request time
    today = datetime.today().date()
    random_seconds = random.randint(0, 86399)  # Random seconds in a full day
    request_time = datetime.combine(today, datetime.min.time()) + timedelta(seconds=random_seconds)

    # Generate pickup and drop times
    pickup_time = request_time + timedelta(minutes=random.randint(1, 5))
    drop_time = pickup_time + timedelta(minutes=int(distance_km * 2))  

    # Ensure drop_time is after pickup_time
    drop_time = max(drop_time, pickup_time + timedelta(minutes=1))

    # Convert times to formatted strings
    time_format = "%Y-%m-%d %H:%M:%S"
    
    ride_event = {
        "ride_id": ride_id,
        "user_id": user_id,
        "driver_id": driver_id,
        "pickup_location": pickup_location,
        "drop_location": drop_location,
        "fare": fare,
        "distance_km": distance_km,
        "request_time": request_time.strftime(time_format),
        "pickup_time": pickup_time.strftime(time_format),
        "drop_time": drop_time.strftime(time_format),
    }
    
    return ride_event

# COMMAND ----------


