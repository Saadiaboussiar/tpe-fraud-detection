# Setup guide: from zero to loading the Edge data into Databricks

This guide shows you, step by step, how to get **the same environment as Task 1** on your PC.

At the end, you will have:

- Mosquitto running on your PC (the "mail carrier" that moves messages around);
- simulated bank data and simulated transactions;
- the Edge, which has sorted those transactions (valid, rejected, duplicates, suspicious);
- that data uploaded to **your own** Databricks;
- two Delta tables filled: `tpe_fraud.bronze.transactions` and `tpe_fraud.quarantaine.rejets`.

Each person uses **their own Databricks account and workspace**. Everything below is therefore done by each person, on their own side.

> This guide stops at loading the Edge data (Auto Loader). The following steps are not described here.

## Overview

| Phase | What you do | Where |
|---|---|---|
| 1 | Set up the PC, fill in `.env`, start Mosquitto, run the tests | Your PC |
| 2 | Prepare Databricks: folder, catalog, schemas, volumes, upload test, Auto Loader test | Databricks + PC |
| 3 | Generate the bank, simulate the terminals, pass the messages through the Edge | Your PC |
| 4 | Send the Edge and bank files to Databricks | Your PC |
| 5 | Load the files into tables with Auto Loader | Databricks |

---

## Phase 1: set up your PC

### 1.1 Programs to install

| Program | What it is for |
|---|---|
| **Git** | Getting the project |
| **Python 3.10** | Running the project scripts |
| **Docker Desktop** | Running Mosquitto in a "container" (a small isolated box) |

### 1.2 Get the project

Open PowerShell, then type:

```powershell
git clone https://github.com/Saadiaboussiar/tpe-fraud-detection.git
cd tpe-fraud-detection
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
```

What these commands do:

- `git clone` copies the project onto your PC;
- `python -m venv .venv` creates a separate Python space for this project (a "virtual environment");
- `.venv\Scripts\activate` turns it on (you will see `(.venv)` at the start of the line);
- `pip install -r requirements.txt` installs the Python libraries the project needs.

From here on, **run every command from the project folder, with `(.venv)` active.**

### 1.3 Fill in the `.env` file

The `.env` file holds your personal settings (the address of your Databricks, your access key). Create it from the template:

```powershell
copy .env.example .env
```

Then open `.env` and fill it in:

| Variable | What to put | How to get it |
|---|---|---|
| `MQTT_HOST` | `localhost` | Leave as is |
| `MQTT_PORT` | `1883` | Leave as is |
| `DATABRICKS_HOST` | The address of your workspace, for example `https://dbc-xxxxxxxx.cloud.databricks.com` | Log in to Databricks and look at the browser address bar. Copy only the beginning, up to `.com`: no `/` at the end and nothing after it (such as `?o=...`) |
| `DATABRICKS_TOKEN` | Your personal access token | See below |
| `DATABRICKS_VOLUME_IOT` | `/Volumes/tpe_fraud/landing/iot` | Copy as is: the phase 2 script creates this volume with this name |
| `DATABRICKS_VOLUME_BANK` | `/Volumes/tpe_fraud/landing/bank` | Copy as is: same reason |

**There is no "username" field**: the workspace address and the token are enough for Databricks to know who you are.

**Create the token:**

1. In Databricks, click your **profile** (top right), then **Settings**.
2. Open **Developer**, then **Access tokens** (click **Manage** if needed).
3. Click **Generate new token**.
4. Give it a name (for example `tpe-fraud`) and a lifetime (30 days is enough).
5. **Copy the token right away**: Databricks shows it only once. Paste it into `.env`.

The exact menu names can change slightly from one Databricks version to another.

### 1.4 Start Docker and Mosquitto

1. **Open Docker Desktop** and wait until it says it is running.
2. In PowerShell, start Mosquitto:

```powershell
docker compose up -d
docker compose ps
```

`docker compose ps` must show the `mosquitto` container with the state **running**. Mosquitto must stay on for everything that follows.

> **What Mosquitto is.** The simulated terminals (TPE) do not send their messages straight to the Edge: they drop them at Mosquitto, which delivers them. It works like a mailbox between the one who writes and the one who reads.

### 1.5 Run the tests

```powershell
pytest
```

You must see **60 passed**. These tests check that the code works; they do not need Databricks.

If you get `No module named 'generator'`, run `python -m pytest` instead.

---

## Phase 2: prepare Databricks (once)

### 2.1 Create the `tpe_fraud` folder

In Databricks: **Workspace**, then **Users**, then your e-mail address. Click **Create**, then **Folder**, and name it `tpe_fraud`. It will hold the project notebooks.

### 2.2 Know how to import a file into Databricks

You will import several project files. Here is where the button is:

1. Go to **Workspace**, **Users**, your e-mail address, then open the `tpe_fraud` folder.
2. Click the **three small dots** (⋮) at the top right of the page.
3. In the menu, click **Import**.
4. Choose the file on your PC, then confirm.


The file then appears in the folder as a **notebook**: a document made of small "cells" of code, which you run one by one.

### 2.3 Create the catalog, schemas and volumes

1. Import the file `pipeline/sql/00_setup.sql` (method from 2.2).
2. Open it and attach **Serverless** compute (top right).
3. Run it with **Run all**.

If a cell does not run, write `%sql` at the very top of that cell: it tells Databricks the cell is SQL.

This script creates:

- a **catalog** `tpe_fraud` (the big binder for the project);
- **schemas** (drawers inside the binder);
- **volumes** (folders for dropping files), including `iot` and `bank`.

To see the result, open the **Catalog** tab on the left, then `tpe_fraud`. You should find your schemas and, in `landing`, the volumes `iot` and `bank`. Their paths are:

```
/Volumes/tpe_fraud/landing/iot
/Volumes/tpe_fraud/landing/bank
```

This is why you could copy these two paths as they are into `.env`.

### 2.4 Test sending a file to Databricks

On your PC, run:

```powershell
python tests/upload_check.py
```

This script creates a tiny test file and sends it to **both volumes** (`bank` and `iot`), using your token. It then prints the list of files present.

Check in **Catalog**, by opening the volumes, that the test file is there. If it is, the connection between your PC and Databricks works.

If it shows an authentication error, check `DATABRICKS_HOST` (no `/` at the end) and `DATABRICKS_TOKEN` in `.env`.

### 2.5 Test Auto Loader

**Auto Loader** is the Databricks feature that reads the files dropped in a volume and puts them into a table. It remembers which files it has already read and loads only the **new** ones. Before using it for real, we test it on the test file.

1. Import `pipeline/notebooks/00_test_autoloader.py` (method from 2.2).
2. Attach **Serverless** compute and run the notebook.
3. Run it a second time: the number of rows must **not** grow. This proves Auto Loader does not read again what it has already read.

If everything is fine, delete the test table:

```sql
DROP TABLE IF EXISTS tpe_fraud.bronze.autoloader_test;
```

You can also delete the test files in the volumes (the `test` folders) from **Catalog**.

---

## Phase 3: generate the data and pass it through the Edge

Keep Docker and Mosquitto on. Run each command from the project folder, with `(.venv)` active.

### 3.1 Generate the bank

```powershell
python generator/generate_bank.py
pytest tests/test_generator.py
```

The script creates CSV files in `data/bank/`: the clients, the cards and the merchants. It also creates, in `data/`, two small files for the simulator (`card_profiles.json` and `terminals.json`). The test must show **9 passed**.

The randomness is fixed (seed 42): you get exactly the same data as we did.

### 3.2 Simulate the terminals a first time

```powershell
python simulator/simulate_tpe.py
pytest tests/test_simulator.py
```

The simulator builds the transactions of the 10 terminals, with frauds and invalid messages hidden inside, then sends them to Mosquitto. It also writes files in `data/`:

| File | Folder | What it is for |
|---|---|---|
| `fraudes_confirmees.csv` | `data/bank/` | The frauds the bank discovers later (after the customer disputes them) |
| `ground_truth.csv` | `data/sim/` | The complete truth about the injected frauds. It stays on your PC |
| `counts.json` | `data/sim/` | The counts, used to check the next steps |

You must see:

```
messages emis : 8397  (attendus : 8397)
fraudes : 165  |  doublons : 41  |  rejets attendus a l'Edge : 124  |  valides attendus : 8232
```

and the test must show **14 passed**.

At this point nobody is listening to the messages: they are lost, and that is normal. The goal of this first run is to create the files above.

### 3.3 Check that Mosquitto loses no message

Open **two PowerShell windows**, each in the project folder with `(.venv)` active.

Window 1 (start it **first**):

```powershell
python simulator/count_subscriber.py
```

Window 2 (once window 1 shows "en attente"):

```powershell
python simulator/simulate_tpe.py
```

Window 1 counts the messages it receives. When the simulator is done, it must show:

```
recus : 8397   emis : 8397   JSON invalides : 0
OK : recus = emis
```

It also creates the file `data/sim/received.json`.

### 3.4 Pass the messages through the Edge

The Edge is the "inspector" that looks at each message before it leaves for the Cloud:

| What it sees | What it does |
|---|---|
| A broken or invalid message (negative amount, missing field, date in the future…) | **Rejects** it, with the reason |
| A transaction already seen less than 10 minutes ago | **Drops** it as a duplicate |
| More than 5 transactions on the same card within 2 minutes | Keeps it, but **marks it suspicious** |
| Everything else | **Keeps** it |

Nothing is ever blocked: the Edge only sorts and marks. It writes its results as Parquet files in `data/edge_out/transactions/` and `data/edge_out/rejets/`.

Window 1 (start it **first**):

```powershell
python edge/gateway.py --idle 30
```

`--idle 30` means: "stop by yourself 30 seconds after the last message".

Window 2 (once window 1 shows "Edge pret"):

```powershell
python simulator/simulate_tpe.py
```

At the end, window 1 must show:

```
recus=8397  valides=8232  rejetes=124  doublons=41  suspects=33
OK : recus = valides + rejetes + doublons
OK : correspond aux comptages du simulateur
```

The number of Parquet files can vary a little (about 9 for transactions and 1 for rejects). What matters most is: **8232 + 124 + 41 = 8397**.

You also get a file `data/sim/edge_stats.json` with these numbers.

---

## Phase 4: send the files to Databricks

The script `edge/upload.py` copies your files into the volumes of **your** Databricks. It puts nothing in a table: it only drops the files.

| File on your PC | Destination in Databricks |
|---|---|
| `data/edge_out/transactions/*.parquet` | `/Volumes/tpe_fraud/landing/iot/transactions/` |
| `data/edge_out/rejets/*.parquet` | `/Volumes/tpe_fraud/landing/iot/rejets/` |
| `data/bank/*.csv` | `/Volumes/tpe_fraud/landing/bank/` |

It only sends **finished** files (never the ones still being written), checks their size once they arrive, retries on failure, and keeps a note of what it sent so it does not send it twice.

```powershell
python edge/upload.py --dry-run
python edge/upload.py
```

- The first command **lists** the files to send, and sends nothing. Check that it shows about 14 files.
- The second one really sends them. At the end it prints the totals: **8232 rows** in `transactions` and **124 rows** in `rejets`.

Keep the file `data/sim/upload_report.json`: it holds these totals. Also check in **Catalog** that the files are in the volumes.

---

## Phase 5: load the files into tables (Auto Loader)

Two notebooks do the work. Import them into Databricks (method from 2.2):

- `pipeline/notebooks/06a_autoloader_transactions.py`: reads the valid transactions and fills `tpe_fraud.bronze.transactions`;
- `pipeline/notebooks/06b_autoloader_rejets.py`: reads the rejects and fills `tpe_fraud.quarantaine.rejets`.

For each one:

1. Attach **Serverless** compute.
2. Fill in the `expected_rows` field at the top of the notebook: **8232** for `06a`, **124** for `06b` (these are the totals from the upload report).
3. Click **Run all**.

What you must see:

| Notebook | Expected result |
|---|---|
| `06a` | `lignes dans Bronze : 8232`, then `OK : Bronze = 8232 lignes` |
| `06b` | `rejets de niveau Edge ... : 124`, then `OK : Quarantaine = 124 lignes` |

Then run each notebook a second time: the numbers must not change.

You can also query the tables in the SQL Editor:

```sql
SELECT COUNT(*) FROM tpe_fraud.bronze.transactions;
SELECT COUNT(*) FROM tpe_fraud.quarantaine.rejets;
```

**You now have the same environment as Task 1.** The Task 1 proof files are in `docs/proofs/` if you want to compare your numbers.

### Starting over from scratch

If you run the Edge again, the new files have different names. Clear the old state first, otherwise the tables will contain duplicates:

1. On your PC, delete `data/edge_out`, `data/upload_state.json` and `data/sim/counts.json`.
2. In Databricks, drop the tables:

```sql
DROP TABLE IF EXISTS tpe_fraud.bronze.transactions;
DROP TABLE IF EXISTS tpe_fraud.quarantaine.rejets;
```

3. In **Catalog**, delete the `bronze_transactions` and `quarantaine_rejets` folders in the `checkpoints` volume, as well as the old files in `iot/transactions` and `iot/rejets`.

**Never delete a checkpoint on its own**: Auto Loader would think nothing has been read and would load everything again.

Running the simulator again rewrites `fraudes_confirmees.csv`: only do it after emptying the tables.

---

## Basic concepts

**MQTT and topics.** A terminal "publishes" a message on a topic, for example `tpe/TPE-001/transactions`. A program that is interested "subscribes" to that topic. The `+` character replaces one level (`tpe/+/transactions` = the transactions of every terminal) and `#` replaces everything after it. Mosquitto does not keep messages for someone who is not listening yet: this is why the Edge starts **before** the simulator.

**QoS 1.** This is the delivery guarantee we use: the message is sent again until the receiver confirms it. It always arrives, but sometimes twice. This is why the Edge removes duplicates.

**Why Mosquitto now, and Kafka later.** MQTT is light and made for connected objects: it carries the messages from the terminals to the Edge. Kafka is heavier, but it keeps messages for a long time and lets you replay them. It will serve as a buffer between the Edge and the Cloud in the next version.

**Auto Loader and `spark.read`.** `spark.read` reads all the files again every time. Auto Loader remembers what it has read (thanks to a "checkpoint" folder) and reads only the new files. Auto Loader is a Databricks feature, built on top of Spark.

**Delta tables.** Files only arrive in the volume. Delta tables are where the data is stored for good. A write either fully succeeds or does not happen at all, which avoids half-filled tables, and you can read the older versions of a table.

**The `publier()` function.** All the Edge output goes through a single function. Later, the destination can be changed (Kafka instead of files) without rewriting the Edge.

---

## Problems we already hit

| What you see | Why, and what to do |
|---|---|
| The Edge receives only part of the messages (4265, 3924, 5523 instead of 8397) | Mosquitto's waiting queue overflowed while the Edge was writing its files. Fixed in two ways: the Edge uses two threads (one receives, one processes) and `config/mosquitto.conf` contains `max_queued_messages 100000` and `max_inflight_messages 500`. Mosquitto's queue is the most likely cause; we did not test each fix on its own. After any change, restart it: `docker compose restart mosquitto` |
| `PARSE_SYNTAX_ERROR` in a notebook | The cell is read as SQL while it is Python. Write `%python` at the top of the cell |
| `CF_FILE_FORMAT_REQUIRED` | The `cloudFiles.format` option did not reach Spark. Keep a single cell, with straight quotes, and run it again |
| `ModuleNotFoundError: No module named 'generator'` | Run `python -m pytest` |
| The `mosquitto_sub` command does not return | Normal: a subscriber waits for messages. Publish from another window |
| `the input device is not a TTY` in Git Bash | Use PowerShell |
| `count_subscriber.py` shows `recus : 0` | The simulator was not running at the same time. Start them in two windows |
| The tables have duplicates, or stay empty, after a rerun | Old files, tables or checkpoints were left behind. Follow "Starting over from scratch" |
