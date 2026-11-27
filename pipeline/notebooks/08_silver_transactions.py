# Databricks notebook source
# MAGIC %md
# MAGIC # 08 - Silver : transactions nettoyees, enrichies et etiquetees
# MAGIC
# MAGIC **Entrees** : `bronze.transactions`, `bronze.cartes`, `bronze.commercants`, `bronze.fraudes_confirmees`
# MAGIC **Sorties** :
# MAGIC - `tpe_fraud.silver.transactions` (recalculee en entier a chaque lancement)
# MAGIC - rejets Silver ajoutes dans `tpe_fraud.quarantaine.rejets` avec `niveau_rejet = 'Silver'`
# MAGIC
# MAGIC **Etapes** : dedoublonnage sur `transaction_id`, typage, regles de qualite, jointures carte et commercant, label `is_fraud`.
# MAGIC
# MAGIC **Label** (date de coupure C, maturite M jours) :
# MAGIC - `is_fraud = 1` : la fraude est confirmee avec `date_confirmation <= C`
# MAGIC - `is_fraud = 0` : aucune fraude confirmee et transaction anterieure a C - M jours
# MAGIC - `is_fraud = NULL` : trop recente, on ne sait pas encore (`label_statut = 'non_etiquete'`)
# MAGIC
# MAGIC `type_fraude` et `date_confirmation` ne sont volontairement pas gardes dans Silver (fuite de donnees).
# MAGIC
# MAGIC **Widgets** : `date_coupure` (vide = derniere date de confirmation) et `maturite_jours` (21 = delai maximal de confirmation du simulateur).

# COMMAND ----------

dbutils.widgets.text("date_coupure", "", "Date de coupure AAAA-MM-JJ (vide = derniere confirmation)")
dbutils.widgets.text("maturite_jours", "21", "Delai de maturite (jours)")

# COMMAND ----------

import datetime
from pyspark.sql import functions as F
from pyspark.sql import Window

B = "tpe_fraud.bronze"
SILVER = "tpe_fraud.silver.transactions"
REJETS = "tpe_fraud.quarantaine.rejets"

spark.sql("CREATE SCHEMA IF NOT EXISTS tpe_fraud.silver")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Dedoublonnage
# MAGIC On garde une seule ligne par `transaction_id` (la plus ancienne chargee).

# COMMAND ----------

bronze = spark.table(f"{B}.transactions")
bronze_cols = [c for c in bronze.columns if not c.startswith("_")]

w = Window.partitionBy("transaction_id").orderBy("_ingested_at", "_source_file")
dedup = (bronze
         .withColumn("_rn", F.row_number().over(w))
         .filter("_rn = 1").drop("_rn")
         # copie brute du message, utilisee si la ligne est rejetee plus bas
         .withColumn("message_brut", F.to_json(F.struct(*[F.col(c) for c in bronze_cols]))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Typage et references (cartes, commercants)

# COMMAND ----------

tx = (dedup
      .withColumn("timestamp", F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss'Z'"))
      .withColumn("transaction_date", F.to_date("timestamp"))
      .withColumn("amount", F.col("amount").cast("decimal(12,2)")))

cartes = (spark.table(f"{B}.cartes")
          .dropDuplicates(["card_id"])
          .select(
              "card_id", "client_id", "type_carte", "marque", "ville_habituelle",
              F.col("plafond_transaction").cast("double").alias("plafond_transaction"),
              F.col("plafond_mensuel").cast("double").alias("plafond_mensuel"),
              F.col("plafond_sans_contact").cast("double").alias("plafond_sans_contact"),
              F.to_date("date_expiration").alias("date_expiration"),
              F.col("statut").alias("statut_carte"),
              F.lit(True).alias("_carte_connue")))

commercants = (spark.table(f"{B}.commercants")
               .dropDuplicates(["merchant_id"])
               .select(
                   "merchant_id", "secteur",
                   F.col("ville").alias("ville_commercant"),
                   F.col("statut").alias("statut_commercant"),
                   F.lit(True).alias("_commercant_connu")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Regles de qualite et jointures
# MAGIC Rejets Silver : horodatage illisible, montant non positif, carte inconnue, commercant inconnu.
# MAGIC Une carte `Bloquee` ou un montant au-dessus du plafond ne sont pas rejetes : ce sont des signaux, utilises dans Gold.

# COMMAND ----------

joined = tx.join(cartes, "card_id", "left").join(commercants, "merchant_id", "left")

raison = (F.when(F.col("timestamp").isNull(), "horodatage_invalide")
           .when(F.col("amount").isNull() | (F.col("amount") <= 0), "montant_non_positif")
           .when(F.col("_carte_connue").isNull(), "carte_inconnue")
           .when(F.col("_commercant_connu").isNull(), "commercant_inconnu"))
joined = joined.withColumn("_raison", raison)

valides = joined.filter(F.col("_raison").isNull())
rejetes = joined.filter(F.col("_raison").isNotNull())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Label `is_fraud`

# COMMAND ----------

fraudes = (spark.table(f"{B}.fraudes_confirmees")
           .withColumn("date_confirmation", F.to_date(F.substring("date_confirmation", 1, 10)))
           .groupBy("transaction_id")
           .agg(F.min("date_confirmation").alias("_date_confirmation")))

txt = dbutils.widgets.get("date_coupure").strip()
if txt:
    coupure = datetime.date.fromisoformat(txt)
else:
    coupure = spark.table(f"{B}.fraudes_confirmees") \
        .select(F.max(F.to_date(F.substring("date_confirmation", 1, 10)))).first()[0]
maturite = int(dbutils.widgets.get("maturite_jours"))
seuil_ancien = coupure - datetime.timedelta(days=maturite)
print(f"date de coupure : {coupure} | transactions anterieures au {seuil_ancien} = assez anciennes pour un label 0")

labelled = (valides.join(fraudes, "transaction_id", "left")
            .withColumn("is_fraud",
                        F.when(F.col("_date_confirmation") <= F.lit(coupure), F.lit(1))
                         .when(F.col("_date_confirmation").isNull()
                               & (F.col("transaction_date") <= F.lit(seuil_ancien)), F.lit(0)))
            .withColumn("label_statut",
                        F.when(F.col("is_fraud") == 1, "fraude")
                         .when(F.col("is_fraud") == 0, "legitime")
                         .otherwise("non_etiquete")))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ecriture : Silver et Quarantaine

# COMMAND ----------

silver = labelled.select(
    "transaction_id", "terminal_id", "merchant_id", "card_id", "client_id",
    "timestamp", "transaction_date", "amount", "currency", "entry_mode", "pin_verified",
    "auth_response", "mcc",
    "suspect", "raison_suspect", "gateway_id", "edge_timestamp",
    "type_carte", "marque", "ville_habituelle", "plafond_transaction", "plafond_mensuel",
    "plafond_sans_contact", "date_expiration", "statut_carte",
    "secteur", "ville_commercant", "statut_commercant",
    "is_fraud", "label_statut",
    F.current_timestamp().alias("_processed_at"))

(silver.write.format("delta").mode("overwrite")
       .option("overwriteSchema", "true").saveAsTable(SILVER))

# Rejets Silver : on supprime ceux d'un lancement precedent puis on ajoute (relance sans doublons)
spark.sql(f"DELETE FROM {REJETS} WHERE niveau_rejet = 'Silver'")
(rejetes.select(
    "message_brut",
    F.col("_raison").alias("raison_rejet"),
    F.lit("Silver").alias("niveau_rejet"),
    F.lit(None).cast("string").alias("gateway_id"),
    F.date_format(F.current_timestamp(), "yyyy-MM-dd'T'HH:mm:ss'Z'").alias("date_rejet"),
    "_source_file",
    F.current_timestamp().alias("_ingested_at"))
 .write.format("delta").mode("append").saveAsTable(REJETS))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verification

# COMMAND ----------

n_bronze = spark.table(f"{B}.transactions").count()
n_distinct = spark.table(f"{B}.transactions").select("transaction_id").distinct().count()
n_doublons = n_bronze - n_distinct
n_silver = spark.table(SILVER).count()
n_rej = spark.table(REJETS).filter("niveau_rejet = 'Silver'").count()

print(f"Bronze = {n_bronze} | Silver = {n_silver} | rejets Silver = {n_rej} | doublons retires = {n_doublons}")
assert n_bronze == n_silver + n_rej + n_doublons, "ECART : Bronze != Silver + rejets + doublons"
print("OK : Bronze = Silver + rejets Silver + doublons retires")
print(f"taux d'invalides Silver : {n_rej / n_distinct:.2%}")

s = spark.table(SILVER)
n_fraud_label = s.filter("is_fraud = 1").count()
n_fraud_bronze = spark.table(f"{B}.fraudes_confirmees").select("transaction_id").distinct().count()
n_fraud_sans_tx = (spark.table(f"{B}.fraudes_confirmees").select("transaction_id").distinct()
                   .join(s.select("transaction_id"), "transaction_id", "left_anti").count())
print(f"is_fraud = 1 : {n_fraud_label} | fraudes confirmees : {n_fraud_bronze} | "
      f"fraudes sans transaction dans Silver : {n_fraud_sans_tx}")
assert n_fraud_label <= n_fraud_bronze, "ECART : plus de labels 1 que de fraudes confirmees"

# COMMAND ----------

display(s.groupBy("label_statut").count())

# COMMAND ----------

# Controle a la main : quelques fraudes connues, avec ce que l'Edge a vu (suspect)
display(s.filter("is_fraud = 1")
         .select("transaction_id", "card_id", "timestamp", "amount", "suspect", "raison_suspect", "is_fraud")
         .orderBy("timestamp").limit(15))

# COMMAND ----------

# Croisement label x suspect de l'Edge (sert a la baseline de l'etape 11)
display(s.filter("is_fraud IS NOT NULL").groupBy("suspect", "is_fraud").count().orderBy("suspect", "is_fraud"))