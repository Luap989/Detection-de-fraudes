import pandas as pd

# -----------------------------
# 📌 Configuration
# -----------------------------
INPUT_FILE = "transaction_header_history.csv"  # Fichier brut
OUTPUT_FILE = "transaction_header_history_bis.csv"  # Fichier nettoyé

# -----------------------------
# 📌 Étape 1 : Charger le fichier CSV
# -----------------------------
df = pd.read_csv(INPUT_FILE, low_memory=False)

# Supprimer les colonnes inutiles (qui contiennent "Unnamed")
df = df.loc[:, ~df.columns.str.contains("^Unnamed")]

# -----------------------------
# 📌 Étape 2 : Correction des types
# -----------------------------

# Convertir les dates au format YYYY-MM-DD
df["purchase_date"] = pd.to_datetime(df["purchase_date"], errors="coerce").dt.date
df["gift_card_purchase_date"] = pd.to_datetime(df["gift_card_purchase_date"], errors="coerce").dt.date

# Convertir les booléens
df["paid_with_credit_card"] = df["paid_with_credit_card"].astype(bool)
df["paid_with_gift_card"] = df["paid_with_gift_card"].astype(bool)

# Convertir `nb_gift_card_used` en entier
df["nb_gift_card_used"] = pd.to_numeric(df["nb_gift_card_used"], errors="coerce").fillna(0).astype(int)

# -----------------------------
# 📌 Étape 3 : Sauvegarde du fichier nettoyé
# -----------------------------
df.to_csv(OUTPUT_FILE, index=False)

print(f"\n✅ Fichier nettoyé sauvegardé sous : {OUTPUT_FILE}")