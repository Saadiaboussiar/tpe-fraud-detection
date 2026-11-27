# Databricks notebook source
# MAGIC %md
# MAGIC # 09 - Gold : table de features et agregat pour les KPIs
# MAGIC
# MAGIC **Entree** : `tpe_fraud.silver.transactions`
# MAGIC **Sorties** (recalculees en entier a chaque lancement) :
# MAGIC - `tpe_fraud.gold.features_tx` : une ligne par transaction, utilisee par le modele (etape 11)
# MAGIC - `tpe_fraud.gold.kpi_heure_commercant` : agregat par heure et par commercant (etapes 10 et 12)
# MAGIC
# MAGIC **Regle anti-fuite de donnees** : les features d'une transaction n'utilisent que des transactions de la meme carte
# MAGIC **strictement anterieures** (la transaction courante et celles du meme instant sont exclues).
# MAGIC `is_fraud` et `label_statut` sont recopies pour l'entrainement et l'evaluation, mais ne servent jamais a calculer une feature.
# MAGIC `suspect` (regle de rafale de l'Edge) est gardee a part : l'etape 11 compare le modele avec et sans elle.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

SILVER = "tpe_fraud.silver.transactions"
FEATURES = "tpe_fraud.gold.features_tx"
KPI = "tpe_fraud.gold.kpi_heure_commercant"

spark.sql("CREATE SCHEMA IF NOT EXISTS tpe_fraud.gold")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Features calculees avec le passe seulement

# COMMAND ----------

s = (spark.table(SILVER)
     .withColumn("amount_d", F.col("amount").cast("double"))
     .withColumn("ts_epoch", F.col("timestamp").cast("long")))     # secondes

# fenetres par carte, triees par temps ; la borne -1 exclut la ligne courante et les lignes du meme instant
w_passe = Window.partitionBy("card_id").orderBy("ts_epoch").rangeBetween(Window.unboundedPreceding, -1)
w_5min = Window.partitionBy("card_id").orderBy("ts_epoch").rangeBetween(-300, -1)

moyenne_passee = F.avg("amount_d").over(w_passe)

features = (s
    .withColumn("nb_tx_passees", F.count(F.lit(1)).over(w_passe))
    .withColumn("nb_tx_5min", F.count(F.lit(1)).over(w_5min))
    .withColumn("moyenne_passee", moyenne_passee)
    .withColumn("ratio_montant_moyenne",
                F.when(F.col("moyenne_passee") > 0, F.col("amount_d") / F.col("moyenne_passee")))   # NULL si aucun historique
    .withColumn("heure", F.hour("timestamp"))
    .withColumn("est_nuit", (F.col("heure") < 6).cast("int"))
    .withColumn("jour_semaine", F.dayofweek("timestamp"))
    .withColumn("ratio_plafond", F.col("amount_d") / F.col("plafond_transaction"))
    .withColumn("carte_bloquee", (F.col("statut_carte") == "Bloquee").cast("int"))
    .select(
        "transaction_id", "card_id", "merchant_id", "terminal_id", "timestamp", "transaction_date",
        F.col("amount_d").alias("amount"),
        "ratio_montant_moyenne", "nb_tx_5min", "nb_tx_passees",
        "heure", "est_nuit", "jour_semaine", "ratio_plafond",
        "entry_mode", "pin_verified", "auth_response", "carte_bloquee",
        "suspect",
        "is_fraud", "label_statut"))

(features.write.format("delta").mode("overwrite")
         .option("overwriteSchema", "true").saveAsTable(FEATURES))
print(f"{FEATURES} : {spark.table(FEATURES).count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Agregat pour les KPIs (par heure et par commercant)

# COMMAND ----------

kpi = (spark.table(SILVER)
       .groupBy(F.date_trunc("hour", "timestamp").alias("heure"), "merchant_id", "secteur")
       .agg(
           F.count(F.lit(1)).alias("nb_transactions"),
           F.sum(F.col("amount").cast("double")).alias("montant_total"),
           F.sum(F.col("suspect").cast("int")).alias("nb_suspectes_edge"),
           F.sum(F.when(F.col("is_fraud") == 1, 1).otherwise(0)).alias("nb_fraudes"),
           F.sum(F.when(F.col("is_fraud").isNotNull(), 1).otherwise(0)).alias("nb_etiquetees"),
           F.sum(F.when(F.col("suspect") & (F.col("is_fraud") == 1), 1).otherwise(0)).alias("nb_suspectes_fraude"),
           F.sum(F.when(F.col("suspect") & (F.col("is_fraud") == 0), 1).otherwise(0)).alias("nb_suspectes_legitimes")))

(kpi.write.format("delta").mode("overwrite")
    .option("overwriteSchema", "true").saveAsTable(KPI))
print(f"{KPI} : {spark.table(KPI).count()} lignes")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

n_silver = spark.table(SILVER).count()
f = spark.table(FEATURES)
n_feat = f.count()
n_ids = f.select("transaction_id").distinct().count()
assert n_feat == n_silver == n_ids, f"ECART : Silver={n_silver}, features={n_feat}, ids distincts={n_ids}"
print(f"OK : features_tx = Silver = {n_silver} lignes, un transaction_id par ligne")

k = spark.table(KPI)
tot = k.agg(F.sum("nb_transactions"), F.sum("nb_fraudes"), F.sum("nb_suspectes_edge")).first()
n_fraud_silver = spark.table(SILVER).filter("is_fraud = 1").count()
n_susp_silver = spark.table(SILVER).filter("suspect").count()
assert tot[0] == n_silver and tot[1] == n_fraud_silver and tot[2] == n_susp_silver, "ECART dans l'agregat KPI"
print(f"OK : l'agregat KPI retrouve {tot[0]} transactions, {tot[1]} fraudes, {tot[2]} suspectes de l'Edge")

n_null = f.filter("ratio_montant_moyenne IS NULL").count()
print(f"transactions sans historique (ratio NULL) : {n_null} (premiere transaction de chaque carte)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Recalcul a la main des features sur une carte de test
# MAGIC On prend la carte qui a la plus forte valeur de `nb_tx_5min` (une rafale) et on recalcule en Python.

# COMMAND ----------

import numpy as np

carte_test = f.orderBy(F.desc("nb_tx_5min")).first()["card_id"]
print("carte de test :", carte_test)

brut = (spark.table(SILVER).filter(F.col("card_id") == carte_test)
        .select("transaction_id", F.col("timestamp").cast("long").alias("t"),
                F.col("amount").cast("double").alias("amount"))
        .toPandas().sort_values(["t", "transaction_id"]).reset_index(drop=True))

lignes = []
for _, r in brut.iterrows():
    passe = brut[brut["t"] < r["t"]]                           # strictement anterieur
    moyenne = passe["amount"].mean() if len(passe) else np.nan
    lignes.append({
        "transaction_id": r["transaction_id"],
        "nb_passees_main": len(passe),
        "nb_5min_main": int((passe["t"] >= r["t"] - 300).sum()),
        "ratio_main": r["amount"] / moyenne if len(passe) and moyenne > 0 else np.nan})
main = __import__("pandas").DataFrame(lignes)

table = (f.filter(F.col("card_id") == carte_test)
          .select("transaction_id", "timestamp", "amount", "nb_tx_passees", "nb_tx_5min", "ratio_montant_moyenne")
          .toPandas())
cmp = main.merge(table, on="transaction_id")

assert (cmp["nb_passees_main"] == cmp["nb_tx_passees"]).all(), "ECART nb_tx_passees"
assert (cmp["nb_5min_main"] == cmp["nb_tx_5min"]).all(), "ECART nb_tx_5min"
assert np.allclose(cmp["ratio_main"].fillna(-1), cmp["ratio_montant_moyenne"].astype(float).fillna(-1)), "ECART ratio"
print(f"OK : les 3 features recalculees a la main correspondent sur les {len(cmp)} transactions de la carte {carte_test}")
display(spark.createDataFrame(cmp.sort_values("timestamp").head(15)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Coup d'oeil : les features separent-elles les fraudes ?

# COMMAND ----------

display(f.filter("is_fraud IS NOT NULL").groupBy("is_fraud").agg(
    F.count(F.lit(1)).alias("nb"),
    F.round(F.avg("ratio_montant_moyenne"), 2).alias("ratio_montant_moyen"),
    F.round(F.avg("nb_tx_5min"), 2).alias("nb_tx_5min_moyen"),
    F.round(F.avg("est_nuit"), 2).alias("part_nuit"),
    F.round(F.avg("ratio_plafond"), 3).alias("ratio_plafond_moyen")))