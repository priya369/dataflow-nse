from nsepython import equity_history
from google.cloud import pubsub_v1
import datetime
import json
import time

# Pub/Sub configuration
PUBSUB_TOPIC = 'projects/data-oasis-472909-u4/topics/dataops-nse-topic'  # Replace with your Pub/Sub topic
publisher = pubsub_v1.PublisherClient()

# List of Nifty 50 stock symbols
nifty_50_symbols = ["LT", "ASIANPAINT", "DMART", "HCLTECH", "MARUTI", "WIPRO", "HDFCLIFE", 
                    "SUNPHARMA", "TITAN", "ULTRACEMCO", "NESTLEIND", "POWERGRID", "ONGC", "JSWSTEEL", "BAJAJFINSV", "COALINDIA", "NTPC"]

end_date = datetime.datetime.now().strftime("%d-%m-%Y")
start_date = (datetime.datetime.now() - datetime.timedelta(days=65)).strftime("%d-%m-%Y")

for symbol in nifty_50_symbols:
    try:
        df = equity_history(symbol, "EQ", start_date, end_date)
        # Convert the DataFrame to JSON format and publish each row to Pub/Sub
        for index, row in df.iterrows():
            message = {
                'symbol': symbol,
                'date': row['CH_TIMESTAMP'],
                'open': row['CH_OPENING_PRICE'],
                'high': row['CH_TRADE_HIGH_PRICE'],
                'low': row['CH_TRADE_LOW_PRICE'],
                'close': row['CH_CLOSING_PRICE'],
                'vwap': row['VWAP']
            }
            # Publish the message to Pub/Sub
            publisher.publish(PUBSUB_TOPIC, json.dumps(message).encode('utf-8'))
            print(f"Published to Pub/Sub: {message}")
        
        # Wait for 5 minutes before processing the next symbol
        time.sleep(300)
        
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
