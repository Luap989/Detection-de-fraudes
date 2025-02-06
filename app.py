from flask import Flask, request
from google.cloud import storage, bigquery
import base64
import json
import os
import uuid

app = Flask(__name__)

# Initialize clients
storage_client = storage.Client()
bigquery_client = bigquery.Client()

# Define dataset and table
DATASET_ID = "transactions_dataset"
TABLE_ID = "transactions_partitioned"

@app.route("/", methods=["POST"])
def handle_pubsub():
    """Handles Pub/Sub messages and loads cleaned data into BigQuery using LoadJob()."""

    # 1️ Parse the Pub/Sub message
    envelope = request.get_json()
    if not envelope:
        return "Bad Request: No Pub/Sub message received", 400
    
    pubsub_message = envelope.get("message", {})
    if "data" not in pubsub_message:
        return "Bad Request: No Pub/Sub message data", 400
    
    # 2️ Decode the Pub/Sub message
    message_data = json.loads(base64.b64decode(pubsub_message["data"]).decode("utf-8"))
    bucket_name = message_data.get("bucket")
    file_name = message_data.get("name")

    # 3️ Confirm bucket matches expected bucket
    if bucket_name != "retailfrauddetectionai-event-driven-bucket":
        return f"File is from unexpected bucket: {bucket_name}", 400

    # 4️ Construct GCS file path
    source_uri = f"gs://{bucket_name}/{file_name}"

    # 5️ Define a unique job ID to prevent conflicts
    job_id = f"load_{file_name.replace('.', '_')}_{uuid.uuid4().hex[:8]}"

    # 6️ Define BigQuery Load Job configuration (to skip first two columns)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        autodetect=True,  # Let BigQuery infer schema
        schema_update_options=[bigquery.SchemaUpdateOption.ALLOW_FIELD_ADDITION],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to table
        projection_fields=["column3", "column4", "column5"]  # Skip first two columns
    )

    # 7️ Load the cleaned data into BigQuery
    try:
        load_job = bigquery.LoadJob(
            job_id=job_id,
            source_uris=[source_uri],
            destination=bigquery_client.dataset(DATASET_ID).table(TABLE_ID),
            client=bigquery_client,
            job_config=job_config
        )
        
        load_job.result()  # Wait for job to complete
        print(f"✅ Successfully loaded cleaned data from {file_name} into {DATASET_ID}.{TABLE_ID}")

    except Exception as e:
        print(f"Error loading {file_name} into BigQuery: {str(e)}")
        return f"Internal Server Error: {str(e)}", 500

    return f"File {file_name} cleaned and loaded into {DATASET_ID}.{TABLE_ID} successfully.", 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)
