# Guide de démarrage et de passation : Tâche 1 → Tâche 2

À lire en premier. Ce guide explique ce qui existe, comment obtenir le même environnement sur votre PC, comment vérifier qu'il correspond au nôtre, et ce que vous construisez ensuite (étapes 7 à 9). Le [README](../README.md) (en anglais) contient la référence complète ; ce guide est le parcours dans l'ordre.

## 1. À décider d'abord : quel workspace Databricks ?

La Tâche 2 se fait surtout **dans Databricks**, sur des tables que la Tâche 1 a déjà remplies. Vous avez besoin de ces tables.

| | Option A : workspace partagé | Option B : votre propre workspace |
|---|---|---|
| Principe | Vous vous connectez au workspace utilisé par la Tâche 1 | Vous en créez un et vous reconstruisez les données |
| Données | Déjà là (Bronze, Quarantaine, CSV de la banque) | Vous relancez toute la chaîne de la Tâche 1 (section 4) |
| Effort | Faible | Environ une demi-journée, et un PC capable de faire tourner Mosquitto, le simulateur et l'Edge ensemble |
| Point d'attention | Le workspace doit vous accepter. **À vérifier : nous n'avons pas vérifié si un workspace gratuit accepte plusieurs utilisateurs** | La même graine donne les mêmes comptages, mais les noms de fichiers et les horodatages diffèrent |

Mettez-vous d'accord avec la Tâche 1 sur l'option **avant** de faire quoi que ce soit d'autre. Tout ce qui suit vaut pour les deux options ; la section 4 ne concerne que l'option B.

## 2. Installer votre PC

Nécessaire dans les deux options : Git, Python 3.10.

```powershell
git clone https://github.com/<utilisateur>/tpe-fraud-detection.git
cd tpe-fraud-detection
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
pytest
```

- `pytest` doit afficher **60 passed**. S'il échoue avec `No module named 'generator'`, lancez `python -m pytest`.
- Remplissez `.env` avec vos propres valeurs : `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_VOLUME_IOT`, `DATABRICKS_VOLUME_BANK` (et les deux lignes MQTT). Vérifiez que `.env.example` contient les deux variables `VOLUME`, et non l'ancienne variable unique `DATABRICKS_VOLUME_PATH`.
- **Ne commitez jamais `.env`.** Si un jeton arrive sur GitHub, révoquez-le tout de suite dans Databricks.
- Si `pip install -r requirements.txt` n'arrive pas à lire le fichier, il est probablement enregistré en UTF-16. Réenregistrez-le en UTF-8.
- Docker Desktop n'est nécessaire que pour l'option B.

## 3. Préparer Databricks (une seule fois)

Dans cet ordre. Option A : sautez ce qui existe déjà.

1. Dans **Workspace**, créez un dossier `tpe_fraud` et importez-y les notebooks (**Import**, choisissez le fichier `.py`). Un fichier qui commence par `# Databricks notebook source` s'ouvre comme un notebook avec des cellules séparées.
2. Importez `pipeline/sql/00_setup.sql`. Mettez `%sql` en tête de chaque cellule SQL et `%python` en tête de chaque cellule Python, pour que le bon langage s'exécute. Lancez-le. Il crée le catalogue `tpe_fraud`, les schémas et les Volumes (`landing/iot`, `landing/bank`, `landing/checkpoints`).
3. Attachez le calcul **Serverless**.
4. En local, vérifiez le chemin d'envoi avec `python tests/upload_check.py`. Un fichier de test doit apparaître dans le Volume.
5. Importez `pipeline/notebooks/00_test_autoloader.py` et lancez-le. Relancez-le : le nombre de lignes ne doit pas augmenter. Puis nettoyez :

```sql
DROP TABLE IF EXISTS tpe_fraud.bronze.autoloader_test;
```

Supprimez ensuite le dossier de checkpoint de test et les fichiers de test (`iot/test`, `bank/test`) dans l'interface Catalog.

## 4. Reconstruire les données (option B seulement)

Depuis la racine du projet, avec `.venv` activé. **L'ordre compte.**

| # | Commande | Attendu |
|---|---|---|
| 1 | `docker compose up -d` | Mosquitto démarré (`docker compose ps`) |
| 2 | `python generator/generate_bank.py` | 150 clients, 200 cartes, 20 commerçants, 10 terminaux |
| 3 | `python edge/gateway.py --idle 30` | Démarre l'Edge et attend. **À lancer avant le simulateur** |
| 4 | `python simulator/simulate_tpe.py` (second terminal) | `messages emis : 8397`, 165 fraudes, 41 doublons, 124 rejets attendus, 8232 valides attendus |
| 5 | Attendre l'arrêt de l'Edge | `recus=8397 valides=8232 rejetes=124 doublons=41 suspects=33`, puis deux lignes `OK` |
| 6 | `python edge/upload.py --dry-run`, puis `python edge/upload.py` | 14 fichiers envoyés ; totaux : 8232 lignes (`transactions`), 124 lignes (`rejets`) |
| 7 | Notebooks `06a`, puis `06b`, dans Databricks | Widget `expected_rows` = 8232, puis 124 ; les deux affichent `OK` |

Règles qui font gagner du temps :

- Un message publié sans abonné est perdu. L'Edge écoute en premier.
- Repartez d'un état propre. Si vous relancez l'Edge, supprimez `data/edge_out`, `data/upload_state.json` et `data/sim/counts.json`, supprimez les deux tables Bronze, les dossiers de checkpoint `bronze_transactions` et `quarantaine_rejets` et les anciens fichiers du Volume. Ne supprimez jamais un checkpoint seul : Auto Loader relirait tous les fichiers.
- Relancer le simulateur réécrit `fraudes_confirmees.csv`. Ne le faites qu'après avoir vidé Bronze.

## 5. Vérifier que votre environnement correspond

| Vérification | Où | Attendu |
|---|---|---|
| Transactions Bronze | `SELECT COUNT(*) FROM tpe_fraud.bronze.transactions` | 8232 |
| Quarantaine, niveau Edge | `SELECT COUNT(*) FROM tpe_fraud.quarantaine.rejets WHERE niveau_rejet = 'Edge'` | 124 |
| Fichiers sources lus | sortie de `06a` | 9 (le nombre peut différer légèrement lors d'une reconstruction) |
| Relance des deux notebooks | | Les comptages ne changent pas |
| Fichiers de la banque dans le Volume | **Catalog**, `landing`, `bank` | `clients.csv`, `cartes.csv`, `commercants.csv`, `fraudes_confirmees.csv` |

Les preuves de la Tâche 1 sont dans `docs/proofs/` (`counts.json`, `received.json`, `edge_stats.json`, `upload_report.json`).

## 6. Votre point de départ (Tâche 2 : étapes 7, 8, 9)

### Ce qui existe déjà dans Databricks

| Objet | Contenu |
|---|---|
| `tpe_fraud.bronze.transactions` | 8232 transactions valides de l'Edge, avec `suspect`, `raison_suspect`, `gateway_id`, `edge_timestamp`, `_source_file`, `_ingested_at`. Les horodatages sont des **chaînes** (`2026-01-05T09:14:03Z`) |
| `tpe_fraud.quarantaine.rejets` | 124 rejets de l'Edge : `message_brut`, `raison_rejet`, `niveau_rejet`, `gateway_id`, `date_rejet`, plus `_source_file`, `_ingested_at` |
| `/Volumes/tpe_fraud/landing/bank` | `clients.csv` (150 lignes), `cartes.csv` (200), `commercants.csv` (20), `fraudes_confirmees.csv` (165) |

Les valeurs sont écrites sans accents : `entry_mode` vaut `puce`, `sans_contact`, `bande_magnetique` ou `saisie_manuelle` ; le statut d'une carte vaut `Active` ou `Bloquee` ; celui d'un commerçant vaut `Actif`.

### Étape 7 : notebook batch de la banque (Bronze)

- Lisez les CSV avec `spark.read` (pas Auto Loader : le cahier exige un mode streaming et un mode batch, et celui-ci est le batch).
- Écrivez des tables `bronze`. Noms proposés, à confirmer en équipe : `tpe_fraud.bronze.clients`, `.cartes`, `.commercants`, `.fraudes_confirmees`.
- Les référentiels (clients, cartes, commerçants) sont remplacés à chaque chargement, avec une date de chargement (snapshot). Les fraudes confirmées sont ajoutées avec un `MERGE` Delta sur `fraud_id`, pour qu'une relance ne crée pas de doublons.
- **Critère :** chaque table a autant de lignes que son CSV ; une relance n'ajoute rien.

### Étape 8 : Silver

- Dédupliquez sur `transaction_id`, convertissez les types (horodatages à partir des chaînes), appliquez les règles de qualité.
- Joignez transaction, carte (plafond, ville habituelle) et commerçant (secteur, ville). Une carte ou un commerçant inconnu va dans `tpe_fraud.quarantaine.rejets` avec `niveau_rejet = 'Silver'` et la raison.
- Construisez le label : `is_fraud = 1` si le `transaction_id` figure dans les fraudes confirmées **et** si `date_confirmation` est antérieure à la date de coupure ; `0` si la transaction est assez ancienne pour en être sûr ; pas de label si elle est trop récente.
- **Critère :** Bronze = Silver + quarantaine Silver + doublons retirés ; vérifiez le label à la main sur quelques transactions connues.

### Étape 9 : Gold

- Agrégats pour les KPIs (par exemple par heure et par commerçant).
- Table `features_tx`, avec **uniquement des informations passées** : montant divisé par la moyenne passée de la carte, nombre de transactions de la carte sur les 5 dernières minutes, mode de saisie, heure, rapport au plafond. Une feature ne doit jamais utiliser d'information postérieure à sa propre transaction (fuite de données).
- **Critère :** recalculez une feature à la main sur une carte de test.

### Règles sur les données

- `ground_truth.csv` et `type_fraude` ne doivent **jamais** entrer dans le pipeline ni dans le modèle.
- **Décidez la date de coupure avant de coder Silver, et écrivez-la dans `docs/`.** La simulation couvre du 2026-01-05 au 2026-02-08 et les délais de confirmation atteignent 21 jours. Avec une coupure en fin de période, seules les transactions antérieures à environ le 2026-01-18 sont sûrement légitimes. Cela fait environ 13 jours de labels fiables, ce qui est juste pour une séparation chronologique entre entraînement et test. Si l'équipe veut plus de données, régénérez avec `python simulator/simulate_tpe.py --days 90` **maintenant**, avant que la Tâche 2 ne s'appuie dessus. Tous les comptages de ce guide changeraient alors : mettez à jour les preuves.
- Mettez les notebooks dans `pipeline/notebooks/`, numérotés dans l'ordre d'exécution (`07_...`, `08_...`, `09_...`).
- Branche : `git switch main`, `git pull`, `git switch -c task2/lakehouse`. Une branche par étape, fusionnée par une pull request relue par la personne suivante.

## 7. Notions à connaître (en bref)

**MQTT.** Un publisher envoie un message sur un topic (`tpe/<terminal_id>/transactions`) ; un subscriber demande au broker (Mosquitto) un filtre de topics. `+` remplace un niveau (`tpe/+/transactions`), `#` remplace tous les niveaux restants. Le broker ne conserve pas les messages pour un abonné absent, sauf si la session est persistante. QoS 0 envoie au plus une fois, QoS 1 au moins une fois (des doublons sont possibles), QoS 2 exactement une fois. Nous utilisons QoS 1, donc l'Edge déduplique.

**Pourquoi Mosquitto, puis Kafka plus tard.** MQTT est un protocole léger pour les objets connectés. Kafka conserve les messages de façon durable dans des topics ordonnés et permet de les rejouer, mais ses clients sont plus lourds. MQTT transporte donc les données des terminaux vers l'Edge, et Kafka (MVP 2) devient le tampon durable entre l'Edge et le Cloud.

**Auto Loader et `spark.read`.** `spark.read.parquet` relit tous les fichiers à chaque exécution. Auto Loader (`cloudFiles`) se souvient de ce qu'il a lu, grâce à son checkpoint, et ne charge que les nouveaux fichiers. Nous utilisons Auto Loader pour le flux IoT et `spark.read` pour les fichiers de la banque. Auto Loader est une fonction de Databricks construite sur Spark Structured Streaming, qui ne fait pas partie de Spark open source.

**Tables Delta.** Les fichiers ne font qu'arriver dans le Volume. À partir de Bronze, tout lit et écrit des tables Delta, qui apportent les transactions ACID, `MERGE`, l'historique des versions et le contrôle du schéma. Sur Databricks, `toTable(...)` crée par défaut une table Delta.

**`publier()`.** Toute la sortie de l'Edge passe par une seule fonction, pour que Kafka puisse remplacer l'écriture Parquet au MVP 2 sans réécrire l'Edge.

## 8. Problèmes déjà rencontrés

| Symptôme | Cause et solution |
|---|---|
| L'Edge ne reçoit qu'une partie du flux (4265, 3924, 5523 sur 8397) | La file d'attente de Mosquitto (1000 messages, 20 en vol) a débordé pendant que l'Edge écrivait des fichiers. Corrigé par un thread de traitement dans l'Edge et, dans `config/mosquitto.conf`, `max_queued_messages 100000` et `max_inflight_messages 500`. La file du broker est la cause décisive la plus probable ; nous n'avons pas testé chaque correction seule. Redémarrez le broker après modification : `docker compose restart mosquitto` |
| `[PARSE_SYNTAX_ERROR]` près de `functions` dans un notebook | La cellule s'est exécutée en SQL. Passez le notebook en Python, ou commencez la cellule par `%python` |
| `CF_FILE_FORMAT_REQUIRED` | L'option `cloudFiles.format` n'est pas arrivée à Spark. Gardez une seule cellule, avec des guillemets droits, et relancez |
| `ModuleNotFoundError: No module named 'generator'` | Lancez `python -m pytest`, ou ajoutez `pytest.ini` avec `pythonpath = .` |
| `docker compose exec ... mosquitto_sub` n'affiche rien | Normal : un abonné attend. Publiez depuis un second terminal |
| `mosquitto_sub` échoue avec « not a TTY » dans Git Bash | Utilisez PowerShell, ou ajoutez `winpty` devant |
| `git switch` refusé : « local changes would be overwritten » | Commitez ou faites un `git stash` d'abord |
| `count_subscriber.py` affiche `recus : 0` | Le simulateur ne tournait pas en même temps. Lancez-les dans deux terminaux |
| Bronze a des doublons ou reste vide après une relance | Des fichiers, tables ou checkpoints anciens sont restés. Suivez la règle d'état propre de la section 4 |

## 9. Règles de travail

- Aucun secret dans Git : `.env` est ignoré, seul `.env.example` est commité.
- `data/` n'est pas versionné. Les petits fichiers de preuve vont dans `docs/proofs/` ; `ground_truth.csv` reste en local.
- Le dépôt est la référence : un notebook modifié dans Databricks doit être exporté et commité.