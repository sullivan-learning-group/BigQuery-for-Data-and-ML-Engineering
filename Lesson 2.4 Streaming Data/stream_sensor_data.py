# Install the BigQuery client library if it's not already installed
!pip install --upgrade google-cloud-bigquery

from google.cloud import bigquery
import json
from datetime import datetime
import time
import random
from google.colab import auth

# Authenticate with Google Cloud
auth.authenticate_user()
project_id = "bq-data-ml-engineering"

# BigQuery table details
dataset_id = "sensor_dataset"      # Replace with your BigQuery dataset ID
table_id = "temp_humidity"          # Replace with your BigQuery table ID
table_full_id = f"{project_id}.{dataset_id}.{table_id}"

# Initialize the BigQuery client
client = bigquery.Client(project=project_id)

def generate_sensor_data():
    """Generates a dictionary representing sensor data."""
    timestamp = datetime.utcnow().isoformat()
    sensor_id = random.randint(1, 10)
    temperature = random.randint(20, 35)
    humidity = random.randint(40, 80)
    return {
        "timestamp": timestamp,
        "sensor_id": sensor_id,
        "temperature": temperature,
        "humidity": humidity,
    }

def stream_data_to_bigquery(data):
    """Streams a list of JSON data to BigQuery."""
    errors = client.insert_rows_json(table_full_id, data)
    if errors == []:
        print(f"Successfully streamed {len(data)} rows to {table_full_id}.")
    else:
        print(f"Errors occurred while streaming {len(data)} rows:")
        for error in errors:
            print(error)

if __name__ == "__main__":
    while True:
        # Generate a batch of data (e.g., 10 rows)
        batch_size = 10
        data_to_stream = [generate_sensor_data() for _ in range(batch_size)]

        # Stream the data to BigQuery
        stream_data_to_bigquery(data_to_stream)

        # Wait for a few seconds before generating the next batch
        time.sleep(5)