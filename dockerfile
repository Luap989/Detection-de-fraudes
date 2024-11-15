# Utiliser l'image officielle Python comme base
FROM python:3.9-slim

# Définir le répertoire de travail dans le conteneur
WORKDIR /app

# Copier les fichiers du projet dans le conteneur
COPY . /app

# Installer les dépendances
RUN pip install --no-cache-dir -r requirements.txt

# Exposer le port utilisé par l'application (Cloud Run utilise le port 8080 par défaut)
EXPOSE 8080

# Commande pour lancer l'application
CMD ["python", "app.py"]