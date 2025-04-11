import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
import csv
import io

# GCP resources
PROJECT_ID = 'neural-guard-454017-q6'
BUCKET_NAME = 'incoming_ftp'
DATASET = 'anz_ftp_sftp'
TEMP_LOCATION = f'gs://{BUCKET_NAME}/temp/'
REGION = 'us-central1'

# Define pipeline options
options = PipelineOptions(
    runner='DirectRunner',
    project=PROJECT_ID,
    temp_location=TEMP_LOCATION,
    region=REGION
)

# -------------------- PARSE FUNCTIONS --------------------

def parse_retail(line):
    fields = ['txn_id', 'product', 'amount', 'timestamp']
    reader = csv.reader(io.StringIO(line))
    return dict(zip(fields, next(reader)))

def parse_macro(line):
    fields = ['date', 'cash_rate', 'unemployment_rate', 'consumer_price_index']
    reader = csv.reader(io.StringIO(line))
    return dict(zip(fields, next(reader)))

# -------------------- SCHEMAS --------------------

RETAIL_SCHEMA = 'txn_id:STRING,product:STRING,amount:FLOAT,timestamp:TIMESTAMP'
MACRO_SCHEMA = 'date:DATE,cash_rate:FLOAT,unemployment_rate:FLOAT,consumer_price_index:FLOAT'

# -------------------- FILES AND TABLES --------------------

RETAIL_FILE = 'gs://incoming_ftp/ftp-data/retail_transactions-20250408-151835.csv'
MACRO_FILE = 'gs://incoming_ftp/ftp-data/macroeconomics data-20250408-151835.csv'

RETAIL_TABLE = 'transactions_external'
MACRO_TABLE = 'economic_indicators_external'

# -------------------- PIPELINE --------------------

with beam.Pipeline(options=options) as pipeline:
    # Retail transactions pipeline
    (
        pipeline
        | 'Read Retail CSV' >> beam.io.ReadFromText(RETAIL_FILE, skip_header_lines=1)
        | 'Parse Retail' >> beam.Map(parse_retail)
        | 'Write Retail to BQ' >> beam.io.WriteToBigQuery(
            table=RETAIL_TABLE,
            dataset=DATASET,
            project=PROJECT_ID,
            schema=RETAIL_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )

    # Macroeconomics data pipeline
    (
        pipeline
        | 'Read Macro CSV' >> beam.io.ReadFromText(MACRO_FILE, skip_header_lines=1)
        | 'Parse Macro' >> beam.Map(parse_macro)
        | 'Write Macro to BQ' >> beam.io.WriteToBigQuery(
            table=MACRO_TABLE,
            dataset=DATASET,
            project=PROJECT_ID,
            schema=MACRO_SCHEMA,
            create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED,
            write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND
        )
    )
