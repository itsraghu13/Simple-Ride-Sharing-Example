# Databricks notebook source
# MAGIC %run "/Ride-Sharing/source-data-generation"

# COMMAND ----------

conf = {
    'bootstrap.servers': 'BOOTSTRAP_SERVER',  # Replace with actual Bootstrap server
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': 'YOUR_USERNAME',  # Replace with actual username
    'sasl.password': 'YOUR_PASSWORD',  # Replace with actual password
    'client.id': 'raghavendra-laptop'
}

# COMMAND ----------

producer = Producer(conf)

# COMMAND ----------

def delivery_report(err, msg):
    if err is not None:
        print(f"Message Delivery Failed {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# COMMAND ----------

while True:
    ride_event = generate_ride_event()
    ride_event_json = json.dumps(ride_event)

    print(f"Sending Ride Event to Kafka")
    producer.produce(
        'ride-request',
        key = str(ride_event["ride_id"]),
        value = ride_event_json,
        callback = delivery_report
    )
    producer.flush()
    time.sleep(2)

# COMMAND ----------


