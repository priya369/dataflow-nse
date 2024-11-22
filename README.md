# dataflow-nse

Nifty 50 Stock Data Streaming Pipeline
This project streams Nifty 50 historical stock data to Google Cloud Pub/Sub and processes it in real-time using Apache Beam with the Dataflow Runner. The pipeline also writes the processed data to BigQuery for analysis.

Key Features
Fetches Nifty 50 historical data using the nsepython library.
Publishes data to a Pub/Sub topic for real-time streaming.
Processes data using Apache Beam and Google Cloud Dataflow.
Stores cleaned and transformed data in BigQuery for further analytics.
