# Guide de démarrage : de zéro jusqu'au chargement des données de l'Edge dans Databricks

Ce guide vous explique, pas à pas, comment obtenir sur votre PC **le même environnement que celui de la Tâche 1**.

À la fin, vous aurez :

- Mosquitto qui tourne sur votre PC (le « facteur » qui transporte les messages) ;
- des données de banque et de transactions simulées ;
- l'Edge qui a trié ces transactions (valides, rejetées, doublons, suspectes) ;
- ces données envoyées dans **votre** Databricks ;
- deux tables Delta remplies : `tpe_fraud.bronze.transactions` et `tpe_fraud.quarantaine.rejets`.

Chaque personne utilise **son propre compte et son propre workspace Databricks**. Tout ce qui suit est donc à faire par chacun, de son côté.

> Ce guide s'arrête au chargement des données de l'Edge (Auto Loader). Les étapes suivantes ne sont pas décrites ici.

## Vue d'ensemble

| Phase | Ce que vous faites | Où |
|---|---|---|
| 1 | Installer le PC, remplir `.env`, lancer Mosquitto, lancer les tests | Votre PC |
| 2 | Préparer Databricks : dossier, catalogue, schémas, volumes, test d'envoi, test d'Auto Loader | Databricks + PC |
| 3 | Générer la banque, simuler les TPE, faire passer les messages par l'Edge | Votre PC |
| 4 | Envoyer les fichiers de l'Edge et de la banque vers Databricks | Votre PC |
| 5 | Charger les fichiers dans les tables avec Auto Loader | Databricks |

---

## Phase 1 : installer votre PC

### 1.1 Les programmes à installer

| Programme | À quoi il sert |
|---|---|
| **Git** | Récupérer le projet |
| **Python 3.10** | Lancer les scripts du projet |
| **Docker Desktop** | Faire tourner Mosquitto dans un « conteneur » (une petite boîte isolée) |

### 1.2 Récupérer le projet

Ouvrez PowerShell, puis tapez :

```powershell
git clone https://github.com/Saadiaboussiar/tpe-fraud-detection.git
cd tpe-fraud-detection
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

Ce que font ces commandes :

- `git clone` copie le projet sur votre PC ;
- `python -m venv .venv` crée un espace Python à part pour ce projet (« environnement virtuel ») ;
- `.venv\Scripts\activate` active cet espace (vous verrez `(.venv)` au début de la ligne) ;
- `pip install -r requirements.txt` installe les bibliothèques Python dont le projet a besoin.

À partir d'ici, **toutes les commandes se lancent depuis le dossier du projet, avec `(.venv)` actif.**

### 1.3 Remplir le fichier `.env`

Le fichier `.env` contient vos réglages personnels (adresse de votre Databricks, votre clé d'accès). Créez-le à partir du modèle :

```powershell
copy .env.example .env
```

Ouvrez ensuite `.env` et remplissez-le :

| Variable | Quoi mettre | Comment l'obtenir |
|---|---|---|
| `MQTT_HOST` | `localhost` | Laissez tel quel |
| `MQTT_PORT` | `1883` | Laissez tel quel |
| `DATABRICKS_HOST` | L'adresse de votre workspace, par exemple `https://dbc-xxxxxxxx.cloud.databricks.com` | Connectez-vous à Databricks et regardez la barre d'adresse du navigateur. Copiez seulement le début, jusqu'à `.com` : sans `/` à la fin et sans ce qui suit (comme `?o=...`) |
| `DATABRICKS_TOKEN` | Votre jeton d'accès personnel | Voir ci-dessous |
| `DATABRICKS_VOLUME_IOT` | `/Volumes/tpe_fraud/landing/iot` | Copiez tel quel : le script de la phase 2 crée ce volume avec ce nom |
| `DATABRICKS_VOLUME_BANK` | `/Volumes/tpe_fraud/landing/bank` | Copiez tel quel : même raison |

**Il n'y a pas de champ « nom d'utilisateur »** : l'adresse du workspace et le jeton suffisent pour que Databricks sache qui vous êtes.

**Créer le jeton (token) :**

1. Dans Databricks, cliquez sur votre **profil** (en haut à droite), puis **Settings**.
2. Ouvrez **Developer**, puis **Access tokens** (cliquez sur **Manage** si besoin).
3. Cliquez sur **Generate new token**.
4. Donnez-lui un nom (par exemple `tpe-fraud`) et une durée de vie (30 jours suffisent).
5. **Copiez le jeton tout de suite** : Databricks ne l'affiche qu'une seule fois. Collez-le dans `.env`.

Le nom exact des menus peut un peu changer d'une version de Databricks à l'autre.

### 1.4 Lancer Docker et Mosquitto

1. **Ouvrez Docker Desktop** et attendez qu'il indique qu'il est démarré.
2. Dans PowerShell, lancez Mosquitto :

```powershell
docker compose up -d
docker compose ps
```

`docker compose ps` doit montrer le conteneur `mosquitto` avec l'état **running**. Mosquitto doit rester allumé pour toute la suite.

> **Ce qu'est Mosquitto.** Les terminaux (TPE) simulés n'envoient pas leurs messages directement à l'Edge : ils les déposent chez Mosquitto, qui les distribue. C'est comme une boîte aux lettres entre celui qui écrit et celui qui lit.

### 1.5 Lancer les tests

```powershell
pytest
```

Vous devez voir **60 passed**. Ces tests vérifient que le code fonctionne ; ils n'ont pas besoin de Databricks.

S'il affiche `No module named 'generator'`, lancez `python -m pytest` à la place.

---

## Phase 2 : préparer Databricks (une seule fois)

### 2.1 Créer le dossier `tpe_fraud`

Dans Databricks : **Workspace**, puis **Users**, puis votre adresse e-mail. Cliquez sur **Create**, puis **Folder**, et appelez-le `tpe_fraud`. Il servira à ranger les notebooks du projet.

### 2.2 Savoir importer un fichier dans Databricks

Vous allez importer plusieurs fichiers du projet. Voici où se trouve le bouton :

1. Allez dans **Workspace**, **Users**, votre adresse e-mail, puis ouvrez le dossier `tpe_fraud`.
2. Cliquez sur les **trois petits points** (⋮) en haut à droite de la page.
3. Dans le menu, cliquez sur **Import**.
4. Choisissez le fichier sur votre PC, puis validez.

![Menu des trois points avec l'option Import](images/databricks-import-menu.png)

Le fichier apparaît ensuite dans le dossier comme un **notebook** : un document fait de petites « cellules » de code, que l'on lance une par une.

### 2.3 Créer le catalogue, les schémas et les volumes

1. Importez le fichier `pipeline/sql/00_setup.sql` (méthode du 2.2).
2. Ouvrez-le et attachez le calcul **Serverless** (en haut à droite).
3. Lancez-le avec **Run all**.

Si une cellule ne s'exécute pas, écrivez `%sql` tout en haut de cette cellule : cela dit à Databricks que c'est du SQL.

Ce script crée :

- un **catalogue** `tpe_fraud` (le grand classeur du projet) ;
- des **schémas** (des tiroirs dans le classeur) ;
- des **volumes** (des dossiers pour déposer des fichiers), dont `iot` et `bank`.

Pour voir le résultat, ouvrez l'onglet **Catalog** à gauche, puis `tpe_fraud`. Vous devez y trouver vos schémas et, dans `landing`, les volumes `iot` et `bank`. Leurs chemins sont :

```
/Volumes/tpe_fraud/landing/iot
/Volumes/tpe_fraud/landing/bank
```

C'est pour cela que vous avez pu copier ces deux chemins tels quels dans `.env`.

### 2.4 Tester l'envoi d'un fichier vers Databricks

Sur votre PC, lancez :

```powershell
python tests/upload_check.py
```

Ce script crée un tout petit fichier de test et l'envoie dans **les deux volumes** (`bank` et `iot`), à l'aide de votre jeton. Il affiche ensuite la liste des fichiers présents.

Vérifiez dans **Catalog**, en ouvrant les volumes, que le fichier de test y est. Si c'est le cas, la connexion entre votre PC et Databricks fonctionne.

S'il affiche une erreur d'authentification, revoyez `DATABRICKS_HOST` (sans `/` à la fin) et `DATABRICKS_TOKEN` dans `.env`.

### 2.5 Tester Auto Loader

**Auto Loader** est la fonction de Databricks qui lit les fichiers déposés dans un volume et les met dans une table. Elle se souvient des fichiers déjà lus et ne charge que les **nouveaux**. Avant de s'en servir pour de vrai, on la teste sur le fichier de test.

1. Importez `pipeline/notebooks/00_test_autoloader.py` (méthode du 2.2).
2. Attachez le calcul **Serverless** et lancez le notebook.
3. Relancez-le une deuxième fois : le nombre de lignes ne doit **pas** augmenter. Cela prouve qu'Auto Loader ne relit pas ce qu'il a déjà lu.

Si tout va bien, supprimez la table de test :

```sql
DROP TABLE IF EXISTS tpe_fraud.bronze.autoloader_test;
```

Vous pouvez aussi supprimer les fichiers de test dans les volumes (dossiers `test`), depuis **Catalog**.

---

## Phase 3 : générer les données et les faire passer par l'Edge

Gardez Docker et Mosquitto allumés. Chaque commande se lance depuis le dossier du projet, avec `(.venv)` actif.

### 3.1 Générer la banque

```powershell
python generator/generate_bank.py
pytest tests/test_generator.py
```

Le script crée des fichiers CSV dans `data/bank/` : les clients, les cartes et les commerçants. Il crée aussi, dans `data/`, deux petits fichiers pour le simulateur (`card_profiles.json` et `terminals.json`). Le test doit afficher **9 passed**.

Le hasard est fixé (graine 42) : vous obtenez exactement les mêmes données que nous.

### 3.2 Simuler les TPE une première fois

```powershell
python simulator/simulate_tpe.py
pytest tests/test_simulator.py
```

Le simulateur fabrique les transactions des 10 terminaux, avec des fraudes et des messages invalides cachés dedans, puis les envoie à Mosquitto. Il écrit aussi des fichiers dans `data/` :

| Fichier | Dossier | À quoi il sert |
|---|---|---|
| `fraudes_confirmees.csv` | `data/bank/` | Les fraudes que la banque découvre plus tard (après contestation du client) |
| `ground_truth.csv` | `data/sim/` | La vérité complète sur les fraudes injectées. Il reste sur votre PC |
| `counts.json` | `data/sim/` | Les comptages, pour vérifier les étapes suivantes |

Vous devez voir :

```
messages emis : 8397  (attendus : 8397)
fraudes : 165  |  doublons : 41  |  rejets attendus a l'Edge : 124  |  valides attendus : 8232
```

et le test doit afficher **14 passed**.

À ce stade, personne n'écoute les messages : ils sont perdus, et c'est normal. Le but de ce premier lancement est de créer les fichiers ci-dessus.

### 3.3 Vérifier que Mosquitto ne perd aucun message

Ouvrez **deux fenêtres PowerShell**, chacune dans le dossier du projet avec `(.venv)` actif.

Fenêtre 1 (à lancer **en premier**) :

```powershell
python simulator/count_subscriber.py
```

Fenêtre 2 (quand la fenêtre 1 affiche « en attente ») :

```powershell
python simulator/simulate_tpe.py
```

La fenêtre 1 compte les messages reçus. Quand le simulateur a fini, elle doit afficher :

```
recus : 8397   emis : 8397   JSON invalides : 0
OK : recus = emis
```

Elle crée aussi le fichier `data/sim/received.json`.

### 3.4 Faire passer les messages par l'Edge

L'Edge est le « contrôleur » qui regarde chaque message avant qu'il parte vers le Cloud :

| Ce qu'il voit | Ce qu'il fait |
|---|---|
| Message cassé ou invalide (montant négatif, champ manquant, date dans le futur…) | Le **rejette**, avec la raison |
| Une transaction déjà vue il y a moins de 10 minutes | L'**écarte** comme doublon |
| Plus de 5 transactions sur la même carte en 2 minutes | La garde, mais la **marque suspecte** |
| Tout le reste | La **garde** |

Rien n'est jamais bloqué : l'Edge ne fait que trier et marquer. Il écrit ses résultats en fichiers Parquet dans `data/edge_out/transactions/` et `data/edge_out/rejets/`.

Fenêtre 1 (à lancer **en premier**) :

```powershell
python edge/gateway.py --idle 30
```

`--idle 30` veut dire : « arrête-toi tout seul 30 secondes après le dernier message ».

Fenêtre 2 (quand la fenêtre 1 affiche « Edge pret ») :

```powershell
python simulator/simulate_tpe.py
```

À la fin, la fenêtre 1 doit afficher :

```
recus=8397  valides=8232  rejetes=124  doublons=41  suspects=33
OK : recus = valides + rejetes + doublons
OK : correspond aux comptages du simulateur
```

Le nombre de fichiers Parquet peut varier un peu (environ 9 pour les transactions et 1 pour les rejets). Le plus important : **8232 + 124 + 41 = 8397**.

Vous avez aussi un fichier `data/sim/edge_stats.json` avec ces chiffres.

---

## Phase 4 : envoyer les fichiers vers Databricks

Le script `edge/upload.py` copie vos fichiers vers les volumes de **votre** Databricks. Il ne met rien dans une table : il dépose seulement les fichiers.

| Fichier sur votre PC | Destination dans Databricks |
|---|---|
| `data/edge_out/transactions/*.parquet` | `/Volumes/tpe_fraud/landing/iot/transactions/` |
| `data/edge_out/rejets/*.parquet` | `/Volumes/tpe_fraud/landing/iot/rejets/` |
| `data/bank/*.csv` | `/Volumes/tpe_fraud/landing/bank/` |

Il n'envoie que des fichiers **terminés** (jamais ceux en cours d'écriture), vérifie leur taille une fois arrivés, réessaie en cas d'échec, et note ce qu'il a envoyé pour ne pas le renvoyer deux fois.

```powershell
python edge/upload.py --dry-run
python edge/upload.py
```

- La première commande **liste** les fichiers à envoyer, sans rien envoyer. Vérifiez qu'elle affiche environ 14 fichiers.
- La seconde envoie vraiment. À la fin, elle affiche les totaux : **8232 lignes** dans `transactions` et **124 lignes** dans `rejets`.

Gardez le fichier `data/upload_report.json` : il contient ces totaux. Vérifiez aussi dans **Catalog** que les fichiers sont dans les volumes.

---

## Phase 5 : charger les fichiers dans les tables (Auto Loader)

Deux notebooks font le travail. Importez-les dans Databricks (méthode du 2.2) :

- `pipeline/notebooks/06a_autoloader_transactions.py` : lit les transactions valides et remplit `tpe_fraud.bronze.transactions` ;
- `pipeline/notebooks/06b_autoloader_rejets.py` : lit les rejets et remplit `tpe_fraud.quarantaine.rejets`.

Pour chacun :

1. Attachez le calcul **Serverless**.
2. Remplissez le champ `expected_rows` en haut du notebook : **8232** pour `06a`, **124** pour `06b` (ce sont les totaux du rapport d'envoi).
3. Cliquez sur **Run all**.

Ce que vous devez voir :

| Notebook | Résultat attendu |
|---|---|
| `06a` | `lignes dans Bronze : 8232`, puis `OK : Bronze = 8232 lignes` |
| `06b` | `rejets de niveau Edge ... : 124`, puis `OK : Quarantaine = 124 lignes` |

Puis relancez chaque notebook une seconde fois : les nombres ne doivent pas changer.

Vous pouvez aussi interroger les tables dans le SQL Editor :

```sql
SELECT COUNT(*) FROM tpe_fraud.bronze.transactions;
SELECT COUNT(*) FROM tpe_fraud.quarantaine.rejets;
```

**Vous avez maintenant le même environnement que la Tâche 1.** Les fichiers de preuve de la Tâche 1 sont dans `docs/proofs/` si vous voulez comparer vos chiffres.

### Repartir de zéro

Si vous relancez l'Edge, les nouveaux fichiers ont d'autres noms. Videz d'abord l'ancien état, sinon les tables contiendront des doublons :

1. Sur votre PC, supprimez `data/edge_out`, `data/upload_state.json` et `data/sim/counts.json`.
2. Dans Databricks, supprimez les tables :

```sql
DROP TABLE IF EXISTS tpe_fraud.bronze.transactions;
DROP TABLE IF EXISTS tpe_fraud.quarantaine.rejets;
```

3. Dans **Catalog**, supprimez les dossiers `bronze_transactions` et `quarantaine_rejets` du volume `checkpoints`, ainsi que les anciens fichiers de `iot/transactions` et `iot/rejets`.

**Ne supprimez jamais un checkpoint tout seul** : Auto Loader croirait que rien n'a été lu et recharge tout.

Relancer le simulateur réécrit `fraudes_confirmees.csv` : ne le faites qu'après avoir vidé les tables.

---

## Notions de base

**MQTT et les topics.** Un TPE « publie » un message sur un topic, par exemple `tpe/TPE-001/transactions`. Un programme intéressé « s'abonne » à ce topic. Le caractère `+` remplace un niveau (`tpe/+/transactions` = les transactions de tous les terminaux) et `#` remplace tout ce qui suit. Mosquitto ne garde pas les messages pour quelqu'un qui n'écoute pas encore : c'est pour cela que l'Edge démarre **avant** le simulateur.

**QoS 1.** C'est le niveau de garantie que nous utilisons : le message est renvoyé tant que le destinataire n'a pas confirmé. Il arrive toujours, mais parfois en double. C'est pour cela que l'Edge supprime les doublons.

**Pourquoi Mosquitto maintenant, et Kafka plus tard.** MQTT est léger, fait pour les objets connectés : il transporte les messages des terminaux vers l'Edge. Kafka est plus lourd, mais garde les messages longtemps et permet de les rejouer. Il servira de tampon entre l'Edge et le Cloud dans la version suivante.

**Auto Loader et `spark.read`.** `spark.read` relit tous les fichiers à chaque fois. Auto Loader se souvient de ce qu'il a lu (grâce à un dossier « checkpoint ») et ne lit que les nouveaux fichiers. Auto Loader est une fonction propre à Databricks, construite sur Spark.

**Tables Delta.** Les fichiers ne font qu'arriver dans le volume. Les tables Delta sont l'endroit où les données sont rangées pour de bon. Une écriture réussit entièrement ou pas du tout, ce qui évite les tables à moitié remplies, et on peut relire les versions anciennes d'une table.

**La fonction `publier()`.** Toute la sortie de l'Edge passe par une seule fonction. Plus tard, on pourra changer la destination (Kafka au lieu des fichiers) sans réécrire l'Edge.

---

## Problèmes déjà rencontrés

| Ce que vous voyez | Pourquoi, et que faire |
|---|---|
| L'Edge ne reçoit qu'une partie des messages (4265, 3924, 5523 au lieu de 8397) | La file d'attente de Mosquitto débordait pendant que l'Edge écrivait ses fichiers. Corrigé de deux façons : l'Edge utilise deux threads (l'un reçoit, l'autre traite) et `config/mosquitto.conf` contient `max_queued_messages 100000` et `max_inflight_messages 500`. La file de Mosquitto est la cause la plus probable ; nous n'avons pas testé chaque correction seule. Après toute modification, redémarrez : `docker compose restart mosquitto` |
| `PARSE_SYNTAX_ERROR` dans un notebook | La cellule est lue comme du SQL alors que c'est du Python. Écrivez `%python` en haut de la cellule |
| `CF_FILE_FORMAT_REQUIRED` | L'option `cloudFiles.format` n'est pas arrivée à Spark. Gardez une seule cellule, avec des guillemets droits, et relancez |
| `ModuleNotFoundError: No module named 'generator'` | Lancez `python -m pytest` |
| La commande `mosquitto_sub` ne rend pas la main | Normal : un abonné attend les messages. Publiez depuis une autre fenêtre |
| `the input device is not a TTY` dans Git Bash | Utilisez PowerShell |
| `count_subscriber.py` affiche `recus : 0` | Le simulateur ne tournait pas en même temps. Lancez-les dans deux fenêtres |
| Les tables ont des doublons, ou restent vides, après une relance | Des fichiers, tables ou checkpoints anciens sont restés. Suivez « Repartir de zéro » |
