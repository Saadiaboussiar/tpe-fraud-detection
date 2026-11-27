# Databricks notebook source
# MAGIC %md
# MAGIC # 07 - CSV de la banque vers Bronze
# MAGIC
# MAGIC **Entree** : `/Volumes/tpe_fraud/landing/bank/*.csv` (deposes par `edge/upload.py`)
# MAGIC **Sortie** : 4 tables Delta dans `tpe_fraud.bronze` :
# MAGIC `clients`, `cartes`, `commercants`, `fraudes_confirmees`
# MAGIC
# MAGIC - Bronze = donnees brutes : toutes les colonnes restent en texte (le typage se fait dans Silver)
# MAGIC - ajoute `_source_file` et `_ingested_at`
# MAGIC - ce sont de petites tables de reference : chaque lancement les recharge en entier (mode overwrite),
# MAGIC   ce qui permet de relancer le notebook sans doublons

# COMMAND ----------

from pyspark.sql import functions as F

BANK = "/Volumes/tpe_fraud/landing/bank"
FICHIERS = {                     # table -> fichier CSV
    "clients": "clients.csv",
    "cartes": "cartes.csv",
    "commercants": "commercants.csv",
    "fraudes_confirmees": "fraudes_confirmees.csv",
}
ATTENDU = {"clients": 150, "cartes": 200, "commercants": 20, "fraudes_confirmees": 165}

spark.sql("CREATE SCHEMA IF NOT EXISTS tpe_fraud.bronze")

# COMMAND ----------

for table, fichier in FICHIERS.items():
    df = (spark.read
          .option("header", True)
          .option("inferSchema", False)          # tout en texte : Bronze = brut
          .csv(f"{BANK}/{fichier}")
          .withColumn("_source_file", F.col("_metadata.file_path"))
          .withColumn("_ingested_at", F.current_timestamp()))
    (df.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true")
       .saveAsTable(f"tpe_fraud.bronze.{table}"))
    print(f"tpe_fraud.bronze.{table} : {df.count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

ok = True
for table, attendu in ATTENDU.items():
    n = spark.table(f"tpe_fraud.bronze.{table}").count()
    etat = "OK" if n == attendu else "ECART"
    ok = ok and n == attendu
    print(f"{etat} : {table} = {n} lignes (attendu {attendu})")
assert ok, "Un comptage ne correspond pas : verifier les CSV envoyes"

# COMMAND ----------

display(spark.table("tpe_fraud.bronze.fraudes_confirmees").limit(10))