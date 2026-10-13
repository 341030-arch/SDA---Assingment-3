# Assignment 3: Real-Time Stock Tick Pipeline (Kafka → MongoDB Atlas → Atlas Charts)

A real-time streaming pipeline that generates mock stock ticks, streams them through Kafka, stores them in MongoDB Atlas, raises trigger-based alerts, and visualizes everything in a MongoDB Atlas Charts dashboard.

## Architecture

```
producer_script.py  →  Kafka topic: sample_topic  →  consumer.py  →  MongoDB Atlas
 (Faker + random walk)                                (parse + alert rules)   ├─ streaming_db.sample_data (raw ticks)
                                                                              └─ streaming_db.alerts      (triggered alerts)
                                                                                         ↓
                                                                                MongoDB Atlas Charts dashboard
```

## Files

| File | Purpose |
|------|---------|
| `producer_script.py` | Generates mock stock ticks with Faker and a random-walk price per symbol, and sends them to Kafka |
| `consumer.py` | Consumes ticks from Kafka, inserts them into MongoDB, and runs the alert rules |
| `requirements.txt` | Python dependencies |
| `screenshots/` | Dashboard screenshots |

## Alert Logic

1. **Volatility alert**: keeps the last 5 prices per symbol and computes their average. If the current price differs from that average by more than **2%**, an alert is raised.
2. **Price spike alert**: finds the oldest price for the symbol within the last **60 seconds**. If the current price moved more than **3%** from it, an alert is raised.

Both rules run on every incoming tick using an in-memory rolling history per symbol. Alerts are printed to the terminal and saved to `streaming_db.alerts`.

## Dashboard (MongoDB Atlas Charts)

1. Column chart: traded volume by symbol
2. Stacked column chart: alerts by symbol and alert type
3. Line chart: mean price over time per symbol
4. Donut chart: NSE vs BSE share of volume
5. Column chart: mean alert deviation (%) by symbol

Screenshots are in the `screenshots/` folder.

## How to Run

1. Start Kafka (`localhost:9092`).
2. Create a virtual environment and install dependencies:
```
   python -m venv venv
   venv\Scripts\activate
   pip install -r requirements.txt
```
3. Set your MongoDB Atlas connection string as an environment variable (never commit it):
```
   set MONGO_URI=your_atlas_connection_string
```
4. Run the producer and consumer in separate terminals:
```
   python producer_script.py
   python consumer.py
```

## Notes

- No credentials are stored in the code; the MongoDB connection string is read from an environment variable.
- `kafka-python-ng` is used instead of `confluent-kafka` because it is pure Python and installs cleanly on Windows.
