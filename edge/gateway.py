"""Passerelle Edge (MVP 1, etape 4).

S'abonne a  tpe/+/transactions  (MQTT), puis pour chaque message :
  1. verifie que c'est du JSON                       -> sinon rejet  json_invalide
  2. valide champs, types, plages, horodatage        -> sinon rejet  <raison>
  3. deduplique sur transaction_id (fenetre memoire) -> doublon ecarte (compte, non publie)
  4. applique la regle de rafale (> 5 transactions sur une carte en 2 minutes)
     -> marque suspect = True, raison_suspect = "rafale"   (la transaction n'est JAMAIS bloquee)
  5. ajoute gateway_id et edge_timestamp
et publie le resultat via publier() : fichiers Parquet par lots, ecrits en .tmp puis renommes.

publier() est le seul point de sortie : au MVP 2 on pourra le remplacer par une publication
Kafka sans reecrire l'Edge.

  python edge/gateway.py                 # Ctrl+C pour arreter
  python edge/gateway.py --idle 10       # s'arrete 10 s apres le dernier message
"""
import argparse
import json
import math
import os
import queue
import sys
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pyarrow.parquet as pq

TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
MIN_DATE = datetime(2020, 1, 1)
FUTURE_TOLERANCE = timedelta(minutes=5)
ENTRY_MODES = {"puce", "sans_contact", "bande_magnetique", "saisie_manuelle"}
AUTH_RESPONSES = {"approuvee", "refusee"}
FIELD_TYPES = {                       # champ -> type attendu
    "transaction_id": "str", "terminal_id": "str", "merchant_id": "str", "timestamp": "str",
    "amount": "num", "currency": "str", "card_id": "str", "entry_mode": "str",
    "pin_verified": "bool", "auth_response": "str", "mcc": "int",
}

SCHEMAS = {
    "transactions": pa.schema([
        ("transaction_id", pa.string()), ("terminal_id", pa.string()), ("merchant_id", pa.string()),
        ("timestamp", pa.string()), ("amount", pa.float64()), ("currency", pa.string()),
        ("card_id", pa.string()), ("entry_mode", pa.string()), ("pin_verified", pa.bool_()),
        ("auth_response", pa.string()), ("mcc", pa.int64()),
        ("suspect", pa.bool_()), ("raison_suspect", pa.string()),
        ("gateway_id", pa.string()), ("edge_timestamp", pa.string()),
    ]),
    "rejets": pa.schema([
        ("message_brut", pa.string()), ("raison_rejet", pa.string()), ("niveau_rejet", pa.string()),
        ("gateway_id", pa.string()), ("date_rejet", pa.string()),
    ]),
}


def _utcnow():
    return datetime.now(timezone.utc).replace(tzinfo=None)


def _iso(dt):
    return dt.strftime(TS_FORMAT)


def _type_ok(value, kind):
    if kind == "str":
        return isinstance(value, str) and value != ""
    if kind == "bool":
        return isinstance(value, bool)
    if kind == "int":
        return isinstance(value, int) and not isinstance(value, bool)
    if kind == "num":
        return isinstance(value, (int, float)) and not isinstance(value, bool) and math.isfinite(value)
    return False


def validate(msg, now):
    """Retourne (raison_rejet, horodatage). raison_rejet vaut None si le message est valide."""
    if not isinstance(msg, dict):
        return "json_invalide", None
    for field in FIELD_TYPES:
        if field not in msg or msg[field] is None:
            return f"champ_manquant:{field}", None
    for field, kind in FIELD_TYPES.items():
        if not _type_ok(msg[field], kind):
            return f"type_invalide:{field}", None
    if msg["amount"] <= 0:
        return "montant_non_positif", None
    try:
        ts = datetime.strptime(msg["timestamp"], TS_FORMAT)
    except ValueError:
        return "horodatage_invalide", None
    if ts > now + FUTURE_TOLERANCE:
        return "date_future", None
    if ts < MIN_DATE:
        return "horodatage_hors_plage", None
    if msg["entry_mode"] not in ENTRY_MODES:
        return "valeur_invalide:entry_mode", None
    if msg["auth_response"] not in AUTH_RESPONSES:
        return "valeur_invalide:auth_response", None
    return None, ts


class EdgeProcessor:
    def __init__(self, gateway_id, publier, now_fn=_utcnow, burst_max=5, burst_window_s=120,
                 dedup_window_min=10):
        self.gateway_id = gateway_id
        self.publier = publier
        self.now_fn = now_fn
        self.burst_max = burst_max
        self.burst_window = timedelta(seconds=burst_window_s)
        self.dedup_window = timedelta(minutes=dedup_window_min)
        self._seen_ids, self._seen_order = set(), deque()
        self._max_ts = None
        self._card_times = defaultdict(deque)
        self.stats = Counter()
        self.reject_reasons = Counter()
        self.last_message_at = None

    def _reject(self, raw, reason):
        self.stats["rejected"] += 1
        self.reject_reasons[reason.split(":")[0]] += 1
        self.publier("rejets", {
            "message_brut": raw, "raison_rejet": reason, "niveau_rejet": "Edge",
            "gateway_id": self.gateway_id, "date_rejet": _iso(self.now_fn()),
        })

    def _is_duplicate(self, tx_id, ts):
        if self._max_ts is None or ts > self._max_ts:
            self._max_ts = ts
        while self._seen_order and self._seen_order[0][0] < self._max_ts - self.dedup_window:
            _, old_id = self._seen_order.popleft()
            self._seen_ids.discard(old_id)
        if tx_id in self._seen_ids:
            return True
        self._seen_ids.add(tx_id)
        self._seen_order.append((ts, tx_id))
        return False

    def _is_burst(self, card_id, ts):
        times = self._card_times[card_id]
        times.append(ts)
        while times and times[0] < ts - self.burst_window:
            times.popleft()
        return len(times) > self.burst_max

    def handle(self, payload):
        """Traite un message MQTT brut (bytes ou str)."""
        self.stats["received"] += 1
        self.last_message_at = time.time()
        raw = payload.decode("utf-8", errors="replace") if isinstance(payload, bytes) else payload
        try:
            msg = json.loads(raw)
        except ValueError:
            return self._reject(raw, "json_invalide")

        reason, ts = validate(msg, self.now_fn())
        if reason:
            return self._reject(raw, reason)
        if self._is_duplicate(msg["transaction_id"], ts):
            self.stats["duplicates"] += 1
            return

        suspect = self._is_burst(msg["card_id"], ts)
        record = {k: msg[k] for k in FIELD_TYPES}
        record["amount"] = float(record["amount"])
        record.update({"suspect": suspect, "raison_suspect": "rafale" if suspect else None,
                       "gateway_id": self.gateway_id, "edge_timestamp": _iso(self.now_fn())})
        self.stats["validated"] += 1
        if suspect:
            self.stats["suspects"] += 1
        self.publier("transactions", record)


class ParquetSink:
    """Regroupe les enregistrements par lots, ecrit en .tmp puis renomme (fichier toujours complet)."""

    def __init__(self, out_dir, gateway_id, batch_size=1000, batch_seconds=60):
        self.out_dir, self.gateway_id = out_dir, gateway_id
        self.batch_size, self.batch_seconds = batch_size, batch_seconds
        self._buf = {f: [] for f in SCHEMAS}
        self._last_flush = {f: time.monotonic() for f in SCHEMAS}
        self._seq = Counter()
        self._lock = threading.RLock()
        self.files_written, self.rows_written = Counter(), Counter()

    def publier(self, family, record):
        with self._lock:
            self._buf[family].append(record)
            if len(self._buf[family]) >= self.batch_size:
                self._flush(family)

    def flush_if_due(self):
        with self._lock:
            for family in SCHEMAS:
                if self._buf[family] and time.monotonic() - self._last_flush[family] >= self.batch_seconds:
                    self._flush(family)

    def flush_all(self):
        with self._lock:
            for family in SCHEMAS:
                if self._buf[family]:
                    self._flush(family)

    def _flush(self, family):
        records = self._buf[family]
        table = pa.Table.from_pylist(records, schema=SCHEMAS[family])   # schema fixe : memes colonnes dans tous les fichiers
        folder = os.path.join(self.out_dir, family)
        os.makedirs(folder, exist_ok=True)
        self._seq[family] += 1
        name = (f"{family}_{self.gateway_id}_{_utcnow():%Y%m%dT%H%M%S}_"
                f"{self._seq[family]:05d}_{uuid.uuid4().hex[:8]}.parquet")
        final = os.path.join(folder, name)
        tmp = final + ".tmp"
        pq.write_table(table, tmp)
        os.replace(tmp, final)                                         # renommage : le fichier apparait complet
        self.files_written[family] += 1
        self.rows_written[family] += len(records)
        self._buf[family] = []
        self._last_flush[family] = time.monotonic()


def start_worker(proc):
    """Le thread MQTT ne fait que deposer le message dans une file ; ce thread le traite.

    Ainsi l'ecriture d'un fichier Parquet ne bloque jamais la lecture du reseau
    (sinon Mosquitto met les messages en attente, puis les abandonne si la file deborde).
    """
    q = queue.Queue()

    def work():
        while True:
            payload = q.get()
            try:
                if payload is None:
                    return
                proc.handle(payload)
                n = proc.stats["received"]
                if n % 1000 == 0:
                    print(f"  {n} messages traites...")
            finally:
                q.task_done()

    t = threading.Thread(target=work, daemon=True)
    t.start()
    return q, t


def main():
    p = argparse.ArgumentParser(description="Passerelle Edge (validation, deduplication, regle de rafale)")
    p.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    p.add_argument("--topic", default="tpe/+/transactions")
    p.add_argument("--gateway", default="GW-AGADIR")
    p.add_argument("--out", default="data/edge_out")
    p.add_argument("--batch-size", type=int, default=1000, help="ecrit un fichier apres N messages")
    p.add_argument("--batch-seconds", type=float, default=60.0, help="ou apres N secondes")
    p.add_argument("--idle", type=float, default=0.0, help="arret apres N s sans message (0 = jamais)")
    p.add_argument("--counts", default="data/sim/counts.json")
    p.add_argument("--stats", default="data/sim/edge_stats.json")
    a = p.parse_args()

    import paho.mqtt.client as mqtt
    sink = ParquetSink(a.out, a.gateway, a.batch_size, a.batch_seconds)
    proc = EdgeProcessor(a.gateway, sink.publier)

    def on_connect(client, userdata, flags, rc, properties=None):
        print(f"connecte (code {rc}), abonnement a {a.topic}")
        client.subscribe(a.topic, qos=1)

    q, worker = start_worker(proc)
    last_rx = [None]                      # heure de la derniere reception reseau

    def on_message(client, userdata, msg):
        last_rx[0] = time.time()
        q.put(msg.payload)                # rapide : le traitement se fait dans le thread de travail

    try:
        client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f"edge-{a.gateway}")
    except AttributeError:          # paho-mqtt 1.x
        client = mqtt.Client(client_id=f"edge-{a.gateway}")
    client.on_connect, client.on_message = on_connect, on_message
    try:
        client.connect(a.host, a.port, keepalive=60)
    except OSError as exc:
        sys.exit(f"Mosquitto injoignable sur {a.host}:{a.port} ({exc}). Lancez 'docker compose up -d'.")

    client.loop_start()
    print("Edge pret : lancez le simulateur (Ctrl+C pour arreter).")
    try:
        while True:
            time.sleep(0.5)
            sink.flush_if_due()
            if (a.idle and last_rx[0] and time.time() - last_rx[0] > a.idle
                    and q.unfinished_tasks == 0):
                break
    except KeyboardInterrupt:
        print("arret manuel")
    finally:
        client.loop_stop()
        client.disconnect()
        q.join()                          # termine le traitement des messages deja recus
        q.put(None)
        worker.join()
        sink.flush_all()

    s = proc.stats
    result = {
        "gateway_id": a.gateway, "received": s["received"], "validated": s["validated"],
        "rejected": s["rejected"], "duplicates": s["duplicates"], "suspects": s["suspects"],
        "reject_reasons": dict(proc.reject_reasons),
        "files_written": dict(sink.files_written), "rows_written": dict(sink.rows_written),
    }
    ok = s["received"] == s["validated"] + s["rejected"] + s["duplicates"]
    result["gate_received_equals_sum"] = ok

    expected = None
    if os.path.exists(a.counts):
        with open(a.counts, encoding="utf-8") as f:
            c = json.load(f)
        expected = {"validated": c.get("expected_valid_after_edge"),
                    "rejected": c.get("expected_rejected_by_edge"),
                    "duplicates": c.get("expected_duplicates")}
        result["expected_from_simulator"] = expected
    os.makedirs(os.path.dirname(a.stats) or ".", exist_ok=True)
    with open(a.stats, "w", encoding="utf-8") as f:
        json.dump(result, f, indent=2)

    print(f"recus={s['received']}  valides={s['validated']}  rejetes={s['rejected']}  "
          f"doublons={s['duplicates']}  suspects={s['suspects']}")
    print(f"raisons de rejet : {dict(proc.reject_reasons)}")
    print(f"fichiers : {dict(sink.files_written)}  lignes : {dict(sink.rows_written)}")
    print("OK : recus = valides + rejetes + doublons" if ok else "ECART : recus != valides + rejetes + doublons")
    if expected:
        same = all(result[k] == expected[k] for k in expected)
        print("OK : correspond aux comptages du simulateur" if same else
              f"ECART avec le simulateur : attendu {expected}")
        ok = ok and same
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()