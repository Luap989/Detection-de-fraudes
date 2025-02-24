from flask import Flask, request
from google.cloud import bigquery, storage
import os
import traceback
import logging

app = Flask(__name__)

# Configure logging
logging.basicConfig(level=logging.INFO)

# Initialize BigQuery and Storage clients
bigquery_client = bigquery.Client()
storage_client = storage.Client()

# Define dataset, table, and bucket
DATASET_ID = "transactions_dataset"
TABLE_ID = "transactions_partitioned"
BUCKET_NAME = "retailfrauddetectionai-event-driven-bucket"

@app.route("/", methods=["POST"])
def handle_pubsub():
    try:
        logging.info("📌 Début du traitement du fichier CSV...")

        # 1. Lister les fichiers disponibles dans le bucket
        logging.info("📌 Liste des fichiers dans le bucket...")
        blobs = storage_client.list_blobs(BUCKET_NAME)
        csv_file = next((blob.name for blob in blobs if blob.name.endswith(".csv")), None)

        if not csv_file:
            logging.error("❌ Aucun fichier CSV trouvé dans le bucket.")
            return "Aucun fichier CSV trouvé dans le bucket.", 404

        logging.info(f"✅ Fichier CSV trouvé : {csv_file}")

        # 2. Télécharger le fichier CSV
        input_file_path = "/tmp/input.csv"
        logging.info(f"📌 Téléchargement du fichier {csv_file} depuis le bucket...")
        blob = storage_client.bucket(BUCKET_NAME).blob(csv_file)
        blob.download_to_filename(input_file_path)

        logging.info(f"✅ Fichier téléchargé avec succès : {input_file_path}")

        # 3. Configurer le job de chargement BigQuery sans schéma explicite
        logging.info("📌 Configuration du job de chargement BigQuery...")
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Ignorer la ligne d'en-tête
            autodetect=True,  # Laisser BigQuery détecter le schéma
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Ajouter à la table existante
        )

        # 4. Charger les données dans BigQuery
        logging.info("⚡️ Tentative de chargement des données dans BigQuery...")
        try:
            with open(input_file_path, "rb") as source_file:
                load_job = bigquery_client.load_table_from_file(
                    source_file,
                    f"{DATASET_ID}.{TABLE_ID}",
                    job_config=job_config
                )

            # Attendre la fin du job
            logging.info("📌 Attente de la fin du job BigQuery...")
            load_job.result()

            logging.info(f"✅ Données chargées avec succès dans {DATASET_ID}.{TABLE_ID}")
            return f"Fichier chargé avec succès dans {DATASET_ID}.{TABLE_ID}.", 200

        except Exception as bq_error:
            logging.error(f"❌ Erreur lors du chargement dans BigQuery : {str(bq_error)}")
            traceback.print_exc()
            return f"Erreur BigQuery : {str(bq_error)}", 500

    except Exception as e:
        logging.error(f"❌ Erreur générale dans le script : {str(e)}")
        traceback.print_exc()
        return f"Erreur interne : {str(e)}", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"🚀 Lancement de l'application sur le port {port}...")
    app.run(host="0.0.0.0", port=port)