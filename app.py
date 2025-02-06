from flask import Flask, request
from google.cloud import storage, bigquery
import base64
import json
import os

app = Flask(__name__)

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Define dataset and table
DATASET_ID = "transactions_dataset"
TABLE_ID = "transactions_partitioned"


@app.route("/", methods=["POST"])
def handle_pubsub():
    """Handles incoming Pub/Sub messages and loads the data into BigQuery."""
    
    # Parse the Pub/Sub message
    envelope = request.get_json()
    if not envelope:
        return "Bad Request: No Pub/Sub message received", 400
    
    pubsub_message = envelope.get("message", {})
    if "data" not in pubsub_message:
        return "Bad Request: No Pub/Sub message data", 400
    
    # Decode the Pub/Sub message
    message_data = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))
    
    # Extract bucket name and file name
    bucket_name = message_data.get("bucket")
    file_name = message_data.get("name")

    if not bucket_name or not file_name:
        return "Bad Request: Missing bucket or file name in message", 400

    # Confirm the bucket matches the expected bucket
    if bucket_name != "retailfrauddetectionai-event-driven-bucket":
        return f"File is from an unexpected bucket: {bucket_name}", 400

    # Construct the GCS file path
    source_uri = f"gs://{bucket_name}/{file_name}"

    # Define BigQuery job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header
        autodetect=True  # Infer schema automatically
    )

    # Create LoadJob instance
    job = bigquery.LoadJob(
        job_id=f"load_{file_name.replace('.', '_')}",
        source_uris=[source_uri],
        destination=bigquery_client.dataset(DATASET_ID).table(TABLE_ID),
        client=bigquery_client,
        job_config=job_config
    )

    # Execute and wait for the job to complete
    job.result()

    return f"File {file_name} successfully loaded into {DATASET_ID}.{TABLE_ID}.", 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
