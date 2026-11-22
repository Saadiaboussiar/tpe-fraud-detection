# Onboarding and handover guide: Task 1 → Task 2

Read this first. It tells you what exists, how to get the same environment on your PC, how to check it matches ours, and what you build next (steps 7 to 9). The [README](../README.md) has the full reference; this guide is the ordered path.

## 1. Decide first: which Databricks workspace?

Task 2 works mostly **inside Databricks**, on tables Task 1 already filled. You need those tables.

| | Option A: shared workspace | Option B: your own workspace |
|---|---|---|
| Idea | You log in to the workspace Task 1 used | You create your own and rebuild the data |
| Data | Already there (Bronze, Quarantine, bank CSVs) | You rerun the whole Task 1 chain (section 4) |
| Effort | Small | About half a day, and a PC that can run Mosquitto, the simulator and the Edge together |
| Catch | The workspace must let you in. **Check this: we have not verified whether a free workspace can have several users** | Same seed gives the same counts, but file names and timestamps differ |

Agree with Task 1 on the option **before** doing anything else. Everything below works for both; section 4 is only for Option B.

## 2. Set up your PC

Needed for both options: Git, Python 3.10.

```powershell
git clone https://github.com/<user>/tpe-fraud-detection.git
cd tpe-fraud-detection
python -m venv .venv
.venv\Scripts\activate
pip install -r requirements.txt
copy .env.example .env
pytest
```

- `pytest` must show **60 passed**. If it fails with `No module named 'generator'`, run `python -m pytest` instead.
- Fill `.env` with your own values: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `DATABRICKS_VOLUME_IOT`, `DATABRICKS_VOLUME_BANK` (and the two MQTT lines). Check that `.env.example` contains the two `VOLUME` variables, not the old single `DATABRICKS_VOLUME_PATH`.
- **Never commit `.env`.** If a token reaches GitHub, revoke it in Databricks at once.
- If `pip install -r requirements.txt` fails reading the file, it is probably saved in UTF-16. Save it again as UTF-8.
- Docker Desktop is needed only for Option B.

## 3. Databricks one-time setup

Do this in order. Option A: skip anything that already exists.

1. In **Workspace**, create a folder `tpe_fraud` and import the notebooks there (**Import**, choose the `.py` file). A file starting with `# Databricks notebook source` opens as a notebook with separate cells.
2. Import `pipeline/sql/00_setup.sql`. Put `%sql` at the top of each SQL cell, and `%python` at the top of each Python cell, so the right language runs. Run it. It creates the catalog `tpe_fraud`, the schemas and the Volumes (`landing/iot`, `landing/bank`, `landing/checkpoints`).
3. Attach **Serverless** compute.
4. Locally, check the upload path with `python tests/upload_check.py`. A test file must appear in the Volume.
5. Import `pipeline/notebooks/00_test_autoloader.py` and run it. Rerun: the row count must not grow. Then clean up:

```sql
DROP TABLE IF EXISTS tpe_fraud.bronze.autoloader_test;
```

Then delete the test checkpoint folder and the test files (`iot/test`, `bank/test`) in the Catalog interface.

## 4. Rebuild the data (Option B only)

From the project root, with `.venv` active. **Order matters.**

| # | Command | Expected |
|---|---|---|
| 1 | `docker compose up -d` | Mosquitto running (`docker compose ps`) |
| 2 | `python generator/generate_bank.py` | 150 clients, 200 cards, 20 merchants, 10 terminals |
| 3 | `python edge/gateway.py --idle 30` | Starts the Edge and waits. **Start it before the simulator** |
| 4 | `python simulator/simulate_tpe.py` (second terminal) | `messages emis : 8397`, 165 frauds, 41 duplicates, 124 rejects expected, 8232 valid expected |
| 5 | Wait for the Edge to stop | `recus=8397 valides=8232 rejetes=124 doublons=41 suspects=33`, then two `OK` lines |
| 6 | `python edge/upload.py --dry-run`, then `python edge/upload.py` | 14 files sent; totals 8232 rows (`transactions`), 124 rows (`rejets`) |
| 7 | Notebooks `06a`, then `06b`, in Databricks | Widget `expected_rows` = 8232, then 124; both print `OK` |

Rules that save you time:

- A message published with no subscriber is lost. The Edge listens first.
- Start from a clean state. If you rerun the Edge, delete `data/edge_out`, `data/upload_state.json` and `data/sim/counts.json`, drop both Bronze tables, delete the `bronze_transactions` and `quarantaine_rejets` checkpoint folders and the old Volume files. Never delete a checkpoint alone: Auto Loader would read every file again.
- Rerunning the simulator rewrites `fraudes_confirmees.csv`. Do it only after emptying Bronze.

## 5. Check that your environment matches

| Check | Where | Expected |
|---|---|---|
| Bronze transactions | `SELECT COUNT(*) FROM tpe_fraud.bronze.transactions` | 8232 |
| Quarantine, Edge level | `SELECT COUNT(*) FROM tpe_fraud.quarantaine.rejets WHERE niveau_rejet = 'Edge'` | 124 |
| Source files read | `06a` output | 9 (the number can differ slightly on a rebuild) |
| Rerun both notebooks | | Counts do not change |
| Bank files in the Volume | **Catalog**, `landing`, `bank` | `clients.csv`, `cartes.csv`, `commercants.csv`, `fraudes_confirmees.csv` |

Proof files from Task 1 are in `docs/proofs/` (`counts.json`, `received.json`, `edge_stats.json`, `upload_report.json`).

## 6. Your starting point (Task 2: steps 7, 8, 9)

### What already exists in Databricks

| Object | Content |
|---|---|
| `tpe_fraud.bronze.transactions` | 8232 valid transactions from the Edge, with `suspect`, `raison_suspect`, `gateway_id`, `edge_timestamp`, `_source_file`, `_ingested_at`. Timestamps are **strings** (`2026-01-05T09:14:03Z`) |
| `tpe_fraud.quarantaine.rejets` | 124 Edge rejects: `message_brut`, `raison_rejet`, `niveau_rejet`, `gateway_id`, `date_rejet`, plus `_source_file`, `_ingested_at` |
| `/Volumes/tpe_fraud/landing/bank` | `clients.csv` (150 rows), `cartes.csv` (200), `commercants.csv` (20), `fraudes_confirmees.csv` (165) |

Value names are written without accents: `entry_mode` is `puce`, `sans_contact`, `bande_magnetique` or `saisie_manuelle`; card status is `Active` or `Bloquee`; merchant status is `Actif`.

### Step 7: bank batch notebook (Bronze)

- Read the CSVs with `spark.read` (not Auto Loader: the cahier requires both a streaming and a batch mode, and this is the batch one).
- Write `bronze` tables. Suggested names, to confirm with the team: `tpe_fraud.bronze.clients`, `.cartes`, `.commercants`, `.fraudes_confirmees`.
- Referentials (clients, cards, merchants) are replaced at each load, with a load date (snapshot). Confirmed frauds are added with a Delta `MERGE` on `fraud_id`, so a rerun creates no duplicates.
- **Gate:** each table has as many rows as its CSV; a rerun adds nothing.

### Step 8: Silver

- Deduplicate on `transaction_id`, cast types (timestamps from string), apply quality rules.
- Join transaction, card (ceiling, usual city) and merchant (sector, city). An unknown card or merchant goes to `tpe_fraud.quarantaine.rejets` with `niveau_rejet = 'Silver'` and the reason.
- Build the label: `is_fraud = 1` if the `transaction_id` is in the confirmed frauds **and** `date_confirmation` is before the cutoff date; `0` if the transaction is old enough to be sure; no label if it is too recent.
- **Gate:** Bronze = Silver + Silver quarantine + duplicates removed; check the label by hand on a few known transactions.

### Step 9: Gold

- Aggregates for the KPIs (for example by hour and by merchant).
- Table `features_tx`, with **past information only**: amount divided by the card's past average, number of card transactions in the last 5 minutes, entry mode, hour, ratio to the ceiling. A feature may never use information after its own transaction (data leakage).
- **Gate:** recompute one feature by hand on a test card.

### Rules for the data

- `ground_truth.csv` and `type_fraude` must **never** enter the pipeline or the model.
- **Decide the cutoff date before coding Silver, and write it in `docs/`.** The simulation covers 2026-01-05 to 2026-02-08 and confirmation delays reach 21 days. With a cutoff at the end, only transactions before about 2026-01-18 are certainly legitimate. That is roughly 13 days of clean labels, which is thin for a chronological train/test split. If the team wants more data, regenerate with `python simulator/simulate_tpe.py --days 90` **now**, before Task 2 builds on it. All the counts in this guide would then change, so update the proofs.
- Put notebooks in `pipeline/notebooks/`, numbered in execution order (`07_...`, `08_...`, `09_...`).
- Branch: `git switch main`, `git pull`, `git switch -c task2/lakehouse`. One branch per step, merged through a pull request that the next person reviews.

## 7. Concepts you need (short)

**MQTT.** A publisher sends a message to a topic (`tpe/<terminal_id>/transactions`); a subscriber asks the broker (Mosquitto) for a topic filter. `+` replaces one level (`tpe/+/transactions`), `#` replaces all remaining levels. The broker does not store messages for absent subscribers unless the session is persistent. QoS 0 sends at most once, QoS 1 at least once (duplicates are possible), QoS 2 exactly once. We use QoS 1, so the Edge deduplicates.

**Why Mosquitto, then Kafka later.** MQTT is a light protocol for connected objects. Kafka keeps messages durably in ordered topics and allows replay, but its clients are heavier. So MQTT carries data from the terminals to the Edge, and Kafka (MVP 2) becomes the durable buffer between the Edge and the Cloud.

**Auto Loader vs `spark.read`.** `spark.read.parquet` reads every file each time. Auto Loader (`cloudFiles`) remembers what it read, through its checkpoint, and loads only new files. We use Auto Loader for the IoT flow and `spark.read` for the bank files. Auto Loader is a Databricks feature built on Spark Structured Streaming, not part of open-source Spark.

**Delta tables.** Files only land in the Volume. From Bronze on, everything reads and writes Delta tables, which give ACID transactions, `MERGE`, version history and schema enforcement. On Databricks, `toTable(...)` creates a Delta table by default.

**`publier()`.** All Edge output goes through one function, so Kafka can replace the Parquet writer in MVP 2 without rewriting the Edge.

## 8. Problems we already hit

| Symptom | Cause and fix |
|---|---|
| Edge receives only part of the stream (4265, 3924, 5523 of 8397) | Mosquitto's waiting list (1000 messages, 20 in flight) overflowed while the Edge wrote files. Fixed by a worker thread in the Edge and, in `config/mosquitto.conf`, `max_queued_messages 100000` and `max_inflight_messages 500`. The broker queue is the most likely decisive cause; we did not test each change alone. Restart the broker after editing: `docker compose restart mosquitto` |
| `[PARSE_SYNTAX_ERROR]` near `functions` in a notebook | The cell ran as SQL. Switch the notebook language to Python, or start the cell with `%python` |
| `CF_FILE_FORMAT_REQUIRED` | The `cloudFiles.format` option did not reach Spark. Keep one cell, with straight quotes, and rerun |
| `ModuleNotFoundError: No module named 'generator'` | Run `python -m pytest`, or add `pytest.ini` with `pythonpath = .` |
| `docker compose exec ... mosquitto_sub` prints nothing | Normal: a subscriber waits. Publish from a second terminal |
| `mosquitto_sub` fails with "not a TTY" in Git Bash | Use PowerShell, or put `winpty` in front |
| `git switch` refused: "local changes would be overwritten" | Commit or `git stash` first |
| `count_subscriber.py` shows `recus : 0` | The simulator was not running at the same time. Run them in two terminals |
| Bronze has duplicates or stays empty after a rerun | Old files, tables or checkpoints were left. Follow the clean-state rule in section 4 |

## 9. Working rules

- No secrets in Git: `.env` is ignored, only `.env.example` is committed.
- `data/` is not versioned. Small proof files go in `docs/proofs/`; `ground_truth.csv` stays local.
- The repository is the source of truth: a notebook edited in Databricks must be exported and committed.