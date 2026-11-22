# Databricks notebook source
# MAGIC %md
# MAGIC # 06b - Auto Loader : rejets de l'Edge vers la Quarantaine
# MAGIC
# MAGIC **Entree** : `/Volumes/tpe_fraud/landing/iot/rejets` (fichiers Parquet deposes par `edge/upload.py`)
# MAGIC **Sortie** : table Delta `tpe_fraud.quarantaine.rejets` avec `niveau_rejet = 'Edge'`
# MAGIC
# MAGIC Le notebook Silver ajoutera plus tard, dans la meme table, les rejets de niveau `Silver`.
# MAGIC Un checkpoint par source : **ne jamais le supprimer**.
# MAGIC
# MAGIC **Verification** : renseigner le widget `expected_rows` avec le total de lignes `rejets` du rapport d'envoi.

# COMMAND ----------

dbutils.widgets.text("expected_rows", "", "Lignes attendues (rapport d'envoi)")

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

SOURCE = "/Volumes/tpe_fraud/landing/iot/rejets"
CHECKPOINT = "/Volumes/tpe_fraud/landing/checkpoints/quarantaine_rejets"
TABLE = "tpe_fraud.quarantaine.rejets"

spark.sql("CREATE SCHEMA IF NOT EXISTS tpe_fraud.quarantaine")
spark.sql("CREATE VOLUME IF NOT EXISTS tpe_fraud.landing.checkpoints")

SCHEMA = StructType([
    StructField("message_brut", StringType()),
    StructField("raison_rejet", StringType()),
    StructField("niveau_rejet", StringType()),
    StructField("gateway_id", StringType()),
    StructField("date_rejet", StringType()),
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

df = spark.table(TABLE).filter("niveau_rejet = 'Edge'")
n_rows = df.count()
print(f"rejets de niveau Edge dans la Quarantaine : {n_rows}")

expected = dbutils.widgets.get("expected_rows").strip()
if expected:
    assert n_rows == int(expected), f"ECART : Quarantaine={n_rows}, attendu={expected}"
    print(f"OK : Quarantaine = {expected} lignes, comme le rapport d'envoi")
else:
    print("renseignez le widget expected_rows pour verifier le comptage")

# COMMAND ----------

display(df.groupBy("niveau_rejet", "raison_rejet").count().orderBy("count", ascending=False))
display(df.limit(10))