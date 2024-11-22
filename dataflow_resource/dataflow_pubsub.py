import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io.gcp.bigquery import WriteToBigQuery
from apache_beam.io.gcp.pubsub import ReadFromPubSub
import json

PROJECT_ID = 'valid-verbena-437709-h5'
DATASET_ID = 'nifty50'
TABLE_ID = 'nifty50_historical_data_65'
PUBSUB_SUBSCRIPTION = 'projects/valid-verbena-437709-h5/subscriptions/dataops-nse-topic-sub'

table_schema = {
    'fields': [
        {'name': 'symbol', 'type': 'STRING'},
        {'name': 'date', 'type': 'DATE'},
        {'name': 'open', 'type': 'FLOAT'},
        {'name': 'high', 'type': 'FLOAT'},
        {'name': 'low', 'type': 'FLOAT'},
        {'name': 'close', 'type': 'FLOAT'},
        {'name': 'vwap', 'type': 'FLOAT'}
    ]
}

class ParsePubSubMessage(beam.DoFn):
    def process(self, element):
        # Decode and parse JSON message
        message = json.loads(element.decode('utf-8'))
        
        # Ensure required fields are present
        if all(k in message for k in ['symbol', 'date', 'open', 'high', 'low', 'close', 'vwap']):
            yield {
                'symbol': message['symbol'],
                'date': message['date'],
                'open': float(message['open']),
                'high': float(message['high']),
                'low': float(message['low']),
                'close': float(message['close']),
                'vwap': float(message['vwap'])
            }

pipeline_options = PipelineOptions(
    runner='DataflowRunner',
    project=PROJECT_ID,
    region='us-east1',
    temp_location='gs://dataops-dataflow-2024/temp',
    staging_location='gs://dataops-dataflow-2024/staging',
    streaming=True,
    job_name='streaming-pubsub-bq-nifty50-v1',
    num_workers=1,
    max_num_workers=10,
    disk_size_gb=50,
    autoscaling_algorithm='THROUGHPUT_BASED',
    machine_type='n1-standard-4',
    service_account_email='dataops-guru-sa@valid-verbena-437709-h5.iam.gserviceaccount.com'
)

# Define the Apache Beam pipeline
with beam.Pipeline(options=pipeline_options) as p:
    (
        p
        # Read messages from Pub/Sub subscription
        | 'Read from Pub/Sub' >> ReadFromPubSub(subscription=PUBSUB_SUBSCRIPTION)
        # Parse JSON messages
        | 'Parse JSON' >> beam.ParDo(ParsePubSubMessage())
        # Write to BigQuery
        | 'Write to BigQuery' >> WriteToBigQuery(
            table=f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}",
            schema=table_schema,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
