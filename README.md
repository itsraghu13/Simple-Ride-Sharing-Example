# 🚖 Ride-Sharing Real-Time ETL with Kafka & Spark

This project simulates a ride-sharing platform (similar to Uber/Lyft) where ride requests are streamed in real-time using **Kafka** and processed using **Spark Structured Streaming**. The data is then stored in **Delta Lake** for analysis.

---

## 🏗 **Project Architecture**
1. **Ride Request Data Generator (Python)**
   - Simulated API that generates ride request data (pickup, drop, fare, distance, timestamps).
   - Sends ride request events to a Kafka topic (`ride_requests`).

2. **Kafka (Message Broker)**
   - Stores ride request events and streams them in real-time.

3. **Spark Structured Streaming**
   - Reads ride request events from Kafka.
   - Enriches the data (e.g., estimated arrival time, surge pricing).
   - Writes processed data to **Delta Lake**.

4. **Storage: Delta Lake**
   - Stores structured ride data for historical analysis.

---

## ⚡ **Tech Stack**
- **Kafka** (Real-time streaming)
- **Spark Structured Streaming** (Data processing)
- **Delta Lake** (Storage)
- **Python (Faker Library)** (Data simulation)

---

📌 Current Progress
✅ Implemented ride_requests producer.
✅ Created Kafka topic for ride requests.
✅ Set up Spark Structured Streaming to consume ride requests.
⬜ Next Steps:

Store processed data in Delta Lake.
Implement ride_completed events.
Add dashboard for visualization.
