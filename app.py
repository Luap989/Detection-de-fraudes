from flask import Flask, request
from google.cloud import storage, bigquery
import base64
import json
import pandas as pd
from io import BytesIO

app = Flask(__name__)

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Define dataset and table
DATASET_ID = "transactions_dataset"
TABLE_ID = "transactions_partitioned"

@app.route("/", methods=["POST"])
def handle_pubsub():
    # Parse the Pub/Sub message
    envelope = request.get_json()
    if not envelope:
        return "Bad Request: No Pub/Sub message received", 400
    
    pubsub_message = envelope.get("message", {})
    if "data" not in pubsub_message:
        return "Bad Request: No Pub/Sub message data", 400
    
    # Decode the Pub/Sub message
    message_data = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))
    bucket_name = message_data["bucket"]
    file_name = message_data["name"]

    # Confirm bucket matches expected bucket
    if bucket_name != "retailfrauddetectionai-event-driven-bucket":
        return f"File is from unexpected bucket: {bucket_name}", 400

    # Download file from GCS
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    file_data = blob.download_as_bytes()
    
    # Load CSV into Pandas
    df = pd.read_csv(BytesIO(file_data))
    
    # Clean the data: Remove first two columns
    df_cleaned = df.iloc[:, 2:]

    # Save cleaned data back to a CSV in memory
    csv_data = df_cleaned.to_csv(index=False)
    blob.upload_from_string(csv_data, content_type="text/csv")

    # Load data into BigQuery
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header
        autodetect=True  # Infer schema automatically
    )

    table_ref = bigquery_client.dataset(DATASET_ID).table(TABLE_ID)
    load_job = bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{file_name}", table_ref, job_config=job_config
    )
    load_job.result()  # Wait for the job to complete

    return f"File {file_name} cleaned and loaded into {DATASET_ID}.{TABLE_ID} successfully.", 200

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)
    
    