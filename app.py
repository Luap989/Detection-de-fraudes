from flask import Flask, request, jsonify
import json
import base64
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
        logging.info("📌 Réception d'un message Pub/Sub...")

        # Vérifier si le message Pub/Sub est bien reçu
        envelope = request.get_json()
        if not envelope:
            logging.error("❌ Requête sans contenu JSON.")
            return "Requête invalide", 400

        logging.info(f"📌 Message reçu : {json.dumps(envelope, indent=2)}")

        # Extraire le message Pub/Sub
        if "message" not in envelope:
            logging.error("❌ Message Pub/Sub invalide : clé 'message' manquante.")
            return "Message Pub/Sub invalide", 400

        pubsub_message = envelope["message"]
        data = pubsub_message.get("data")

        if not data:
            logging.error("❌ Message Pub/Sub sans 'data'.")
            return "Message Pub/Sub sans 'data'", 400

        # ✅ Correction : Décodage base64 du message Pub/Sub
        try:
            decoded_bytes = base64.b64decode(data)
            decoded_message = json.loads(decoded_bytes.decode("utf-8"))
        except Exception as decode_error:
            logging.error(f"❌ Erreur lors du décodage du message Pub/Sub : {str(decode_error)}")
            return jsonify({"status": "error", "message": "Erreur de décodage du message"}), 400

        logging.info(f"📌 Contenu décodé du message Pub/Sub : {json.dumps(decoded_message, indent=2)}")

        # Récupérer le fichier concerné (dans "name" ou "objectId")
        csv_file = decoded_message.get("name") or decoded_message.get("objectId")

        if not csv_file:
            logging.error("❌ Impossible de récupérer le nom du fichier dans le message Pub/Sub.")
            return "Message Pub/Sub sans nom de fichier", 400

        logging.info(f"✅ Fichier détecté dans le message Pub/Sub : {csv_file}")

        # 2. Télécharger le fichier CSV depuis le bucket
        input_file_path = "/tmp/input.csv"
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
            return jsonify({"status": "success", "message": f"Fichier {csv_file} chargé avec succès"}), 200

        except Exception as bq_error:
            logging.error(f"❌ Erreur lors du chargement dans BigQuery : {str(bq_error)}")
            traceback.print_exc()
            return jsonify({"status": "error", "message": str(bq_error)}), 500

    except Exception as e:
        logging.error(f"❌ Erreur générale dans le script : {str(e)}")
        traceback.print_exc()
        return jsonify({"status": "error", "message": str(e)}), 500


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    logging.info(f"🚀 Lancement de l'application sur le port {port}...")
    app.run(host="0.0.0.0", port=port, debug=False)