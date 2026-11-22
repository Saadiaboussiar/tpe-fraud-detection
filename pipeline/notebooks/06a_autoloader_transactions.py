# Databricks notebook source
# MAGIC %md
# MAGIC # 06a - Auto Loader : transactions valides de l'Edge vers Bronze
# MAGIC
# MAGIC **Entree** : `/Volumes/tpe_fraud/landing/iot/transactions` (fichiers Parquet deposes par `edge/upload.py`)
# MAGIC **Sortie** : table Delta `tpe_fraud.bronze.transactions` (ajout seulement, jamais modifiee)
# MAGIC
# MAGIC - schema explicite (le meme que celui ecrit par l'Edge)
# MAGIC - un dossier de controle (checkpoint) par source : **ne jamais le supprimer** (sinon tout est relu en double)
# MAGIC - ajoute `_source_file` et `_ingested_at`
# MAGIC - mode : lit les nouveaux fichiers puis s'arrete, lance a la main
# MAGIC
# MAGIC **Verification** : renseigner le widget `expected_rows` avec le total de lignes `transactions` du rapport d'envoi
# MAGIC (`data/sim/upload_report.json`, section `totals_since_start`).

# COMMAND ----------

dbutils.widgets.text("expected_rows", "", "Lignes attendues (rapport d'envoi)")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, StringType,
                               StructField, StructType)

SOURCE = "/Volumes/tpe_fraud/landing/iot/transactions"
CHECKPOINT = "/Volumes/tpe_fraud/landing/checkpoints/bronze_transactions"
TABLE = "tpe_fraud.bronze.transactions"

spark.sql("CREATE SCHEMA IF NOT EXISTS tpe_fraud.bronze")
spark.sql("CREATE VOLUME IF NOT EXISTS tpe_fraud.landing.checkpoints")

SCHEMA = StructType([
    StructField("transaction_id", StringType()),
    StructField("terminal_id", StringType()),
    StructField("merchant_id", StringType()),
    StructField("timestamp", StringType()),
    StructField("amount", DoubleType()),
    StructField("currency", StringType()),
    StructField("card_id", StringType()),
    StructField("entry_mode", StringType()),
    StructField("pin_verified", BooleanType()),
    StructField("auth_response", StringType()),
    StructField("mcc", LongType()),
    StructField("suspect", BooleanType()),
    StructField("raison_suspect", StringType()),
    StructField("gateway_id", StringType()),
    StructField("edge_timestamp", StringType()),
])

# COMMAND ----------

query = (
    spark.readStream.format("cloudFiles")
    .option("cloudFiles.format", "parquet")
    .schema(SCHEMA)
    .load(SOURCE)
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
    .writeStream
    .option("checkpointLocation", CHECKPOINT)
    .trigger(availableNow=True)          # lit les nouveaux fichiers, puis s'arrete
    .toTable(TABLE)
)
query.awaitTermination()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

df = spark.table(TABLE)
n_rows = df.count()
n_files = df.select("_source_file").distinct().count()
n_ids = df.select("transaction_id").distinct().count()
print(f"lignes dans Bronze : {n_rows}")
print(f"fichiers sources   : {n_files}")
print(f"transaction_id distincts : {n_ids}  (doublons : {n_rows - n_ids})")

expected = dbutils.widgets.get("expected_rows").strip()
if expected:
    assert n_rows == int(expected), f"ECART : Bronze={n_rows}, attendu={expected}"
    print(f"OK : Bronze = {expected} lignes, comme le rapport d'envoi")
else:
    print("renseignez le widget expected_rows pour verifier le comptage")

# COMMAND ----------

display(df.groupBy("suspect", "raison_suspect").count())
display(df.orderBy("_ingested_at", ascending=False).limit(10))