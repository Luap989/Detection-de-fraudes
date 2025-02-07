from flask import Flask, request
from google.cloud import bigquery, storage
import os
import csv
import uuid
import traceback

app = Flask(__name__)

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
        # 1. Lister les fichiers disponibles dans le bucket et récupérer le premier fichier CSV
        blobs = storage_client.list_blobs(BUCKET_NAME)
        csv_file = None

        for blob in blobs:
            if blob.name.endswith(".csv"):
                csv_file = blob.name
                break

        if not csv_file:
            return "Aucun fichier CSV trouvé dans le bucket.", 404

        print(f"✅ Fichier CSV trouvé : {csv_file}")

        # 2. Télécharger le fichier CSV
        input_file_path = "/tmp/input.csv"
        cleaned_file_path = "/tmp/cleaned.csv"
        blob = storage_client.bucket(BUCKET_NAME).blob(csv_file)
        blob.download_to_filename(input_file_path)

        print(f"✅ Fichier téléchargé : {input_file_path}")

        # 3. Supprimer les deux premières colonnes
        with open(input_file_path, "r") as infile, open(cleaned_file_path, "w", newline="") as outfile:
            reader = csv.reader(infile)
            writer = csv.writer(outfile)

            # Lire l'en-tête et supprimer les deux premières colonnes
            header = next(reader)
            writer.writerow(header[2:])  # Écrire l'en-tête sans les deux premières colonnes

            # Lire et écrire le reste des lignes sans les deux premières colonnes
            for row in reader:
                writer.writerow(row[2:])

        print(f"✅ Fichier nettoyé et sauvegardé : {cleaned_file_path}")

        # 4. Configurer le job de chargement BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.CSV,
            skip_leading_rows=1,  # Skip header row
            autodetect=True,  # Laisser BigQuery déduire le schéma
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # Ajouter à la table existante
        )

        # 5. Charger les données nettoyées dans BigQuery
        with open(cleaned_file_path, "rb") as source_file:
            load_job = bigquery_client.load_table_from_file(
                source_file,
                f"{DATASET_ID}.{TABLE_ID}",
                job_config=job_config
            )

        # Attendre la fin du job
        load_job.result()

        print(f"✅ Données chargées avec succès dans {DATASET_ID}.{TABLE_ID}")
        return f"Fichier chargé avec succès dans {DATASET_ID}.{TABLE_ID}.", 200

    except Exception as e:
        print(f"❌ Erreur lors du chargement : {str(e)}")
        traceback.print_exc()
        return f"Erreur interne : {str(e)}", 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port)