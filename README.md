# Nifty 50 Stock Data Streaming Pipeline

This project streams **Nifty 50 historical stock data** to **Google Cloud Pub/Sub** and processes it in real-time using **Apache Beam** with the **Dataflow Runner**. The pipeline also writes the processed data to **BigQuery** for analysis.

## Key Features
- Fetches Nifty 50 historical data using the **nsepython** library.
- Publishes data to a **Pub/Sub topic** for real-time streaming.
- Processes data using **Apache Beam** and Google Cloud **Dataflow**.
- Stores cleaned and transformed data in **BigQuery** for further analytics.

---

## Components

### 1. Data Publisher
- Fetches historical stock data (last 65 days) for Nifty 50 companies.
- Publishes the data to a Google Cloud Pub/Sub topic in JSON format.

### 2. Data Pipeline
- **Source**: Reads messages from Pub/Sub.
- **Transformations**:
  - Parses JSON messages.
  - Validates and formats stock data.
- **Sink**: Writes the data to a BigQuery table.

---

## Prerequisites
1. **Google Cloud Project** with enabled APIs:
   - Pub/Sub
   - Dataflow
   - BigQuery
2. **Service Account** with permissions:
   - Pub/Sub Admin
   - Dataflow Admin
   - BigQuery Data Editor
3. **Python Libraries**:
   - `apache-beam[gcp]`
   - `google-cloud-pubsub`
   - `nsepython`
   - `pandas`

---

## Deployment Steps

### 1. Clone the Repository
```bash
git clone https://github.com/your-repo/nifty50-streaming.git
cd nifty50-streaming
