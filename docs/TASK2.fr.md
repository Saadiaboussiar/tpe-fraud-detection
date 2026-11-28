# Guide de la Tâche 2 : de Bronze (banque) jusqu'à Gold

Ce guide explique comment faire tourner la **Tâche 2** (étapes 7 à 9) et ce que fait chaque notebook.

À la fin, vous aurez dans **votre** Databricks :

- 4 tables Bronze pour la banque : `clients`, `cartes`, `commercants`, `fraudes_confirmees` ;
- la table `tpe_fraud.silver.transactions` : transactions nettoyées, reliées à la carte et au commerçant, avec le label `is_fraud` ;
- les tables Gold : `tpe_fraud.gold.features_tx` (variables pour le modèle) et `tpe_fraud.gold.kpi_heure_commercant` (agrégats pour les KPIs).

> Ce guide commence là où `docs/ONBOARDING.fr.md` s'arrête. La Tâche 2 se fait **entièrement dans Databricks** : rien à lancer sur le PC, ni Mosquitto, ni l'Edge.

## Vue d'ensemble

| Étape | Notebook (`pipeline/notebooks/`) | Entrée | Sortie |
|---|---|---|---|
| 7 | `07_bronze_banque.py` | les 4 CSV du volume `bank` | 4 tables `tpe_fraud.bronze.*` |
| 8 | `08_silver_transactions.py` | les tables Bronze | `tpe_fraud.silver.transactions` + rejets Silver dans la Quarantaine |
| 9 | `09_gold.py` | `silver.transactions` | `gold.features_tx` et `gold.kpi_heure_commercant` |

Les trois notebooks s'exécutent **à la main, dans cet ordre**. Chacun **réécrit** sa table de sortie : on peut donc les relancer sans créer de doublons (ce qui n'est pas le cas de `06a` et `06b`, qui dépendent de leur checkpoint).

---

## Phase 0 : vérifier les prérequis

Dans le **SQL Editor** de Databricks :

```sql
SELECT COUNT(*) FROM tpe_fraud.bronze.transactions;      -- 8232
SELECT COUNT(*) FROM tpe_fraud.quarantaine.rejets;       -- 124
SHOW VOLUMES IN tpe_fraud.landing;                       -- bank, checkpoints, iot
```

Dans **Catalog**, ouvrez le volume `bank` : il doit contenir `clients.csv`, `cartes.csv`, `commercants.csv` et `fraudes_confirmees.csv`.

Les schémas `silver` et `gold` sont créés par `00_setup.sql`. S'ils manquent :

```sql
CREATE SCHEMA IF NOT EXISTS tpe_fraud.silver;
CREATE SCHEMA IF NOT EXISTS tpe_fraud.gold;
```

Pour importer un notebook, utilisez la même méthode qu'en Tâche 1 (`docs/ONBOARDING.fr.md`, 2.2) : **Workspace**, votre dossier `tpe_fraud`, les trois petits points (⋮), **Import**. Attachez ensuite le calcul **Serverless** et cliquez sur **Run all**.

---

## Étape 7 : Bronze de la banque

Importez et lancez `07_bronze_banque.py`.

Vous devez voir :

```
OK : clients = 150 lignes (attendu 150)
OK : cartes = 200 lignes (attendu 200)
OK : commercants = 20 lignes (attendu 20)
OK : fraudes_confirmees = 165 lignes (attendu 165)
```

**Ce que fait le notebook**

- Il lit les 4 CSV avec `spark.read` : c'est le mode **batch**, volontairement différent d'Auto Loader (le projet demande de montrer les deux modes).
- Toutes les colonnes restent en **texte brut** : le typage se fait en Silver.
- Il ajoute `_source_file` (fichier d'origine) et `_ingested_at` (date de chargement).
- Il recharge la table en entier à chaque lancement (mode `overwrite`) : une relance ne crée aucun doublon.

Le document du projet propose un `MERGE` sur `fraud_id` pour les fraudes confirmées. Avec un seul chargement initial, la table complète suffit. À revoir si les confirmations arrivent un jour par lots successifs.

---

## Étape 8 : Silver

Importez et lancez `08_silver_transactions.py`.

Le notebook a **deux paramètres** (panneau « Parameters » à gauche, bouton **Show parameters** si besoin). **Laissez les valeurs par défaut** :

| Paramètre | Valeur | Rôle |
|---|---|---|
| `date_coupure` | vide | Date à laquelle on « regarde » les fraudes confirmées. Vide = la dernière `date_confirmation` |
| `maturite_jours` | `21` | Délai maximal de confirmation d'une fraude dans le simulateur |

Vous devez voir :

```
Bronze = 8232 | Silver = 8232 | rejets Silver = 0 | doublons retires = 0
OK : Bronze = Silver + rejets Silver + doublons retires
taux d'invalides Silver : 0.00%
is_fraud = 1 : 165 | fraudes confirmees : 165 | fraudes sans transaction dans Silver : 0
```

Puis la répartition des labels : **165** `fraude`, **7380** `legitime`, **687** `non_etiquete`.

**Ce que fait le notebook, dans l'ordre**

| Opération | Détail |
|---|---|
| 1. Dédoublonner | Une seule ligne par `transaction_id` (la plus anciennement chargée). Ici il n'y a aucun doublon, car l'Edge les a déjà retirés |
| 2. Typer | `timestamp` devient un vrai horodatage, `transaction_date` est ajoutée, `amount` devient décimal, les plafonds deviennent des nombres, `date_expiration` une date |
| 3. Contrôler la qualité | Une ligne est rejetée vers `quarantaine.rejets` avec `niveau_rejet = 'Silver'` si : `horodatage_invalide`, `montant_non_positif`, `carte_inconnue` ou `commercant_inconnu` |
| 4. Relier | Transaction + carte (type, marque, ville habituelle, plafonds, expiration, statut) + commerçant (secteur, ville, statut) |
| 5. Étiqueter | Création de `is_fraud` et `label_statut` (règle ci-dessous) |

Une carte **bloquée** ou un montant **au-dessus du plafond** ne sont volontairement **pas** rejetés : ce sont des signaux de fraude, gardés pour les variables de Gold. Avant d'ajouter ses rejets, le notebook supprime ceux de son lancement précédent (niveau `Silver` seulement) : les rejets de l'Edge ne sont jamais touchés.

**La règle du label** (date de coupure C, maturité M = 21 jours)

| Situation | `is_fraud` | `label_statut` |
|---|---|---|
| La transaction est dans `fraudes_confirmees` avec `date_confirmation` ≤ C | 1 | `fraude` |
| Elle n'y est pas **et** elle a plus de M jours avant C | 0 | `legitime` |
| Sinon (trop récente pour savoir) | vide (NULL) | `non_etiquete` |

On ne sait pas immédiatement si un paiement est une fraude : la banque la confirme de 3 à 21 jours après. Une transaction récente absente de la liste n'est donc pas forcément légitime, d'où le label vide.

`type_fraude` et `date_confirmation` **ne sont pas gardés** en Silver : ils n'existent pas au moment du paiement, et les donner au modèle serait une **fuite de données**.

**Colonnes de `silver.transactions`** : `transaction_id`, `terminal_id`, `merchant_id`, `card_id`, `client_id`, `timestamp`, `transaction_date`, `amount`, `currency`, `entry_mode`, `pin_verified`, `auth_response`, `mcc`, `suspect`, `raison_suspect`, `gateway_id`, `edge_timestamp`, `type_carte`, `marque`, `ville_habituelle`, `plafond_transaction`, `plafond_mensuel`, `plafond_sans_contact`, `date_expiration`, `statut_carte`, `secteur`, `ville_commercant`, `statut_commercant`, `is_fraud`, `label_statut`, `_processed_at`.

---

## Étape 9 : Gold

Importez et lancez `09_gold.py` (pas de paramètre).

Vous devez voir :

```
OK : features_tx = Silver = 8232 lignes, un transaction_id par ligne
OK : l'agregat KPI retrouve 8232 transactions, 165 fraudes, 33 suspectes de l'Edge
transactions sans historique (ratio NULL) : ... (premiere transaction de chaque carte)
OK : les 3 features recalculees a la main correspondent sur les ... transactions de la carte ...
```

Le notebook recalcule lui-même, en Python, trois variables sur une carte de test (celle qui a la plus grosse rafale), et vérifie qu'elles correspondent à la table. Il affiche aussi un petit tableau qui compare les moyennes des variables entre fraudes et légitimes.

**Table `gold.features_tx`** : une ligne par transaction.

| Colonne | Définition |
|---|---|
| `ratio_montant_moyenne` | montant ÷ moyenne des montants **passés** de la carte. Vide pour la première transaction d'une carte |
| `nb_tx_5min` | nombre de transactions de la carte dans les 300 secondes **avant** celle-ci |
| `nb_tx_passees` | nombre de transactions passées de la carte |
| `heure`, `est_nuit`, `jour_semaine` | heure (UTC), 1 si l'heure est avant 6 h, jour de la semaine |
| `ratio_plafond` | montant ÷ `plafond_transaction` |
| `entry_mode`, `pin_verified`, `auth_response`, `carte_bloquee` | recopiés (`carte_bloquee` = 1 si la carte est `Bloquee`) |
| `suspect` | règle de rafale de l'Edge, gardée à part pour comparer le modèle avec et sans |
| `is_fraud`, `label_statut` | la cible, recopiée de Silver, jamais utilisée pour calculer une variable |

On garde aussi les identifiants (`transaction_id`, `card_id`, `merchant_id`, `terminal_id`), `timestamp`, `transaction_date` et `amount`.

**Table `gold.kpi_heure_commercant`** : une ligne par heure et par commerçant, avec `nb_transactions`, `montant_total`, `nb_suspectes_edge`, `nb_fraudes`, `nb_etiquetees`, `nb_suspectes_fraude` (suspecte et fraude) et `nb_suspectes_legitimes` (suspecte et légitime).

**Pas d'information du futur.** Pour une transaction faite à 10 h 05, on n'utilise que ce qui s'est passé **avant 10 h 05** sur la même carte. Exemple : une carte a payé 100, 120 puis 900. Pour le troisième paiement, la moyenne passée vaut (100 + 120) ÷ 2 = 110, donc `ratio_montant_moyenne` ≈ 8,2. Le 900 n'entre jamais dans sa propre moyenne. Les transactions du même instant sont aussi exclues.

---

## Résultats attendus (Tâche 2)

| Étape | Contrôle | Résultat |
|---|---|---|
| Bronze banque | nombre de lignes | clients 150, cartes 200, commercants 20, fraudes_confirmees 165 |
| Silver | Bronze = Silver + rejets + doublons retirés | 8232 = 8232 + 0 + 0 |
| Silver | taux d'invalides | 0,00 % |
| Silver | fraudes `is_fraud = 1` et fraudes confirmées | 165 et 165 (aucune fraude sans transaction) |
| Silver | répartition des labels | `fraude` 165, `legitime` 7380, `non_etiquete` 687 |
| Silver | règle de l'Edge contre le label (lignes étiquetées) | `suspect` et fraude : 33 ; non suspecte et fraude : 132 ; non suspecte et légitime : 7380 |
| Gold | vérifié par le notebook à chaque lancement | `features_tx` a autant de lignes que Silver ; l'agrégat KPI retrouve les totaux de Silver ; trois variables recalculées à la main sur une carte |

Les données sont fixées par la graine 42 : vous devez obtenir exactement les mêmes chiffres que ci-dessus.

---

## Notions de base

**Bronze, Silver, Gold.** Trois couches pour ranger les données. *Bronze* : les données brutes, telles qu'elles arrivent. *Silver* : les mêmes données nettoyées, typées et reliées entre elles. *Gold* : des tables prêtes à servir (variables pour le modèle, agrégats pour les KPIs). À chaque passage, on compare les comptages pour vérifier qu'aucune ligne ne disparaît sans explication.

**Label et date de coupure.** Le label `is_fraud` est la réponse que le modèle devra apprendre. Il est reconstruit après coup, avec les dates, à partir des fraudes confirmées par la banque. Changer la date de coupure change les transactions étiquetées : si vous la modifiez, relancez `08` et `09` et tout ce qui en dépend.

**Fuite de données.** Donner au modèle une information qu'il n'aurait pas au moment réel de la prédiction (par exemple le type de fraude, ou un montant futur). Cela donnerait de très bons scores, mais faux.

**Fenêtre glissante.** Un calcul fait sur les transactions d'une même carte, rangées dans l'ordre du temps, qui s'arrête juste avant la transaction courante.

---

## Pour la Tâche 3 (étapes 10 à 13)

Les entrées sont `gold.features_tx` (modèle), `gold.kpi_heure_commercant` (KPIs), `silver.transactions` et `quarantaine.rejets` (taux), plus les fichiers locaux `data/sim/edge_stats.json` et `data/sim/ground_truth.csv`.

1. **Lignes étiquetées seulement.** Filtrez `is_fraud IS NOT NULL` : 7545 lignes (165 fraudes, 7380 légitimes, environ 2,2 % de fraudes). Les 687 lignes non étiquetées sont exclues de l'entraînement et du test.
2. **Découpage chronologique** sur `timestamp` (passé pour l'entraînement, futur pour le test). Tout rééquilibrage se fait après le découpage, sur l'entraînement seulement.
3. **Valeurs vides.** `ratio_montant_moyenne` est vide pour la première transaction de chaque carte : à traiter à l'étape du modèle, après le découpage (par exemple avec une valeur calculée sur l'entraînement).
4. **Baseline = la règle de l'Edge (`suspect`).** Sur les lignes étiquetées, elle marque 33 transactions, toutes de vraies fraudes (précision 100 %), et rate 132 des 165 fraudes (rappel d'environ 20 %). `nb_tx_5min` exprime la même idée que la rafale : comparez le modèle **avec et sans** `suspect` et `nb_tx_5min`, comme le demande le document du projet.
5. **KPIs (étape 10).**
   - Taux de fraude = `SUM(nb_fraudes) / SUM(nb_etiquetees)` sur `gold.kpi_heure_commercant`.
   - Taux de rejet de l'Edge = rejets Edge (`quarantaine.rejets` avec `niveau_rejet = 'Edge'`, soit 124) ÷ messages reçus (8397, dans `edge_stats.json`).
   - Taux de fausses alertes de la baseline = `SUM(nb_suspectes_legitimes) / SUM(nb_suspectes_edge)`.
   - Recalculez chaque KPI à la main sur un petit échantillon.
6. **Données synthétiques.** Les fraudes sont injectées par l'équipe et faciles à repérer : un score très élevé ne prouve pas que le modèle marcherait en vrai. À écrire dans les résultats.
7. **Étape 13 (test de bout en bout).** Choisissez une rafale dans `data/sim/ground_truth.csv` (fichier local, jamais envoyé dans Databricks) et suivez ses transactions : marquage par l'Edge, Bronze, label Silver, variables Gold.
8. Branches conseillées : `task3/kpis`, `task3/model`, `task3/e2e-tests`.

---

## Problèmes à connaître

| Ce que vous voyez | Pourquoi, et que faire |
|---|---|
| `Table or view not found` sur `tpe_fraud.bronze.cartes` (ou une autre table banque) | Le notebook `07` n'a pas été lancé. Lancez-le avant `08` |
| `Table or view not found` sur `quarantaine.rejets` pendant `08` | La Quarantaine n'existe pas encore : lancez `06b` (Tâche 1) |
| `Schema ... silver` ou `gold` introuvable | Lancez `CREATE SCHEMA IF NOT EXISTS tpe_fraud.silver;` et `... gold;` |
| `ECART : Bronze != Silver + rejets + doublons`, ou moins de 165 fraudes | Les chiffres de la Tâche 1 ne sont plus ceux d'origine (simulateur relancé sans vider les tables, par exemple). Revenez à la Phase 0 et suivez « Repartir de zéro » du guide `ONBOARDING.fr.md` |
| Les paramètres de `08` n'apparaissent pas | Cliquez sur **Show parameters** (panneau latéral) ou lancez la première cellule seule, puis relancez |
| Silver affiche **0 rejet** | Normal : les données synthétiques sont cohérentes. Les règles existent mais ne sont pas déclenchées |
| Les labels ne sont plus 165 / 7380 / 687 | La date de coupure a été modifiée. Videz le paramètre `date_coupure` et relancez `08` puis `09` |

**Repartir de zéro (Tâche 2).** Il n'y a rien à supprimer : relancez `07`, `08`, `09` dans l'ordre, ils réécrivent leurs tables. Si vous avez refait la Tâche 1 depuis zéro, faites-le avant, puis relancez ces trois notebooks.
