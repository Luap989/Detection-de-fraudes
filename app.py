from flask import Flask, request
from google.cloud import bigquery
import base64
import json
import os
import uuid
import traceback

app = Flask(__name__)

# Initialize BigQuery client
bigquery_client = bigquery.Client()

# Define dataset and table
DATASET_ID = "transactions_dataset"
TABLE_ID = "transactions_partitioned"

@app.route("/", methods=["POST"])
def handle_pubsub():
    """Handles Pub/Sub messages and loads data directly into BigQuery."""

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

    # 6️ Define BigQuery Load Job configuration
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,  # Skip header row
        autodetect=True,  # Let BigQuery infer schema
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append to table
        allow_jagged_rows=True,
        allow_quoted_newlines=True,
        ignore_unknown_values=True,
        use_avro_logical_types=True  # Active le Character Map V2
)

    # 7️ Load the data into BigQuery
    try:
        load_job = bigquery_client.load_table_from_uri(
            source_uri,
            f"{DATASET_ID}.{TABLE_ID}",
            job_config=job_config
        )

        # Wait for the load job to complete
        load_job.result()

        print(f"✅ Successfully loaded data from {file_name} into {DATASET_ID}.{TABLE_ID}")
        return f"File {file_name} successfully loaded into {DATASET_ID}.{TABLE_ID}.", 200

    except Exception as e:
        print(f"❌ Error loading {file_name} into BigQuery: {str(e)}")
        # Afficher les détails complets de l'exception
        traceback.print_exc()
        return f"Internal Server Error: {str(e)}", 500

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)