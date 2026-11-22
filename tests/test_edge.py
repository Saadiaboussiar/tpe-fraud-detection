"""Verifications de l'etape 4 : validation, deduplication, rafale, fichiers Parquet."""
import json
import os
from datetime import datetime

import pyarrow.parquet as pq
import pytest

from edge.gateway import SCHEMAS, EdgeProcessor, ParquetSink, start_worker
from generator.generate_bank import generate
from simulator.simulate_tpe import build_events

NOW = datetime(2026, 11, 18, 12, 0, 0)


def make(publier=None):
    out = {"transactions": [], "rejets": []}
    pub = publier or (lambda family, rec: out[family].append(rec))
    return EdgeProcessor("GW-TEST", pub, now_fn=lambda: NOW), out


def good(**kw):
    m = {"transaction_id": "TX-1", "terminal_id": "TPE-001", "merchant_id": "M-001",
         "timestamp": "2026-01-05T10:00:00Z", "amount": 120.5, "currency": "MAD", "card_id": "CARD-00001",
         "entry_mode": "puce", "pin_verified": True, "auth_response": "approuvee", "mcc": 5411}
    m.update(kw)
    return json.dumps(m)


def test_valid_message_is_enriched_and_not_suspect():
    proc, out = make()
    proc.handle(good())
    rec = out["transactions"][0]
    assert rec["suspect"] is False and rec["raison_suspect"] is None
    assert rec["gateway_id"] == "GW-TEST" and rec["edge_timestamp"] == "2026-11-18T12:00:00Z"
    assert proc.stats["validated"] == 1


@pytest.mark.parametrize("raw,reason", [
    (good(amount=-5), "montant_non_positif"),
    (good(amount=0), "montant_non_positif"),
    (good(timestamp="2030-01-01T00:00:00Z"), "date_future"),
    (good(timestamp="pas une date"), "horodatage_invalide"),
    (good(entry_mode="telepathie"), "valeur_invalide:entry_mode"),
    (good(amount="12"), "type_invalide:amount"),
    ("ceci n'est pas du json", "json_invalide"),
    ("[1, 2, 3]", "json_invalide"),
])
def test_invalid_messages_are_rejected_with_reason(raw, reason):
    proc, out = make()
    proc.handle(raw)
    assert out["transactions"] == []
    rej = out["rejets"][0]
    assert rej["raison_rejet"] == reason and rej["niveau_rejet"] == "Edge"
    assert rej["message_brut"] == raw and rej["gateway_id"] == "GW-TEST"


@pytest.mark.parametrize("field", ["card_id", "amount", "timestamp", "merchant_id", "transaction_id"])
def test_missing_field_is_rejected(field):
    proc, out = make()
    m = json.loads(good())
    del m[field]
    proc.handle(json.dumps(m))
    assert out["rejets"][0]["raison_rejet"] == f"champ_manquant:{field}"


def test_duplicate_is_dropped_and_counted():
    proc, out = make()
    proc.handle(good())
    proc.handle(good())
    assert len(out["transactions"]) == 1
    assert proc.stats["duplicates"] == 1 and proc.stats["received"] == 2
    assert out["rejets"] == []


def test_burst_flags_from_the_sixth_transaction_within_two_minutes():
    proc, out = make()
    for i in range(8):
        proc.handle(good(transaction_id=f"TX-{i}", timestamp=f"2026-01-05T10:00:{i * 5:02d}Z"))
    flags = [r["suspect"] for r in out["transactions"]]
    assert flags == [False] * 5 + [True] * 3
    assert all(r["raison_suspect"] == "rafale" for r in out["transactions"] if r["suspect"])


def test_no_burst_when_transactions_are_spread_out():
    proc, out = make()
    for i in range(8):
        proc.handle(good(transaction_id=f"TX-{i}", timestamp=f"2026-01-05T{10 + i}:00:00Z"))
    assert not any(r["suspect"] for r in out["transactions"])


def test_suspect_transaction_is_still_published_never_blocked():
    proc, out = make()
    for i in range(7):
        proc.handle(good(transaction_id=f"TX-{i}", timestamp=f"2026-01-05T10:00:{i:02d}Z"))
    assert len(out["transactions"]) == 7


def test_bursts_are_per_card():
    proc, out = make()
    for i in range(10):
        proc.handle(good(transaction_id=f"TX-{i}", card_id=f"CARD-{i:05d}", timestamp="2026-01-05T10:00:00Z"))
    assert not any(r["suspect"] for r in out["transactions"])


@pytest.fixture(scope="module")
def sim_run():
    _, cartes, commercants, profiles, terminaux = generate(seed=42)
    mcc_of = dict(zip(commercants["merchant_id"], commercants["mcc"]))
    events, _, _, counts = build_events(cartes, profiles, terminaux, mcc_of, days=21, seed=42)
    proc, out = make()
    for e in events:
        proc.handle(json.dumps(e["msg"]))
    return events, counts, proc, out


def test_edge_counts_match_simulator_counts(sim_run):
    _, counts, proc, _ = sim_run
    s = proc.stats
    assert s["received"] == counts["emitted_expected"]
    assert s["validated"] == counts["expected_valid_after_edge"]
    assert s["rejected"] == counts["expected_rejected_by_edge"]
    assert s["duplicates"] == counts["expected_duplicates"]
    assert s["received"] == s["validated"] + s["rejected"] + s["duplicates"]


def test_every_injected_burst_is_flagged_from_its_sixth_transaction(sim_run):
    events, _, _, out = sim_run
    flagged = {r["transaction_id"] for r in out["transactions"] if r["suspect"]}
    bursts = {}
    for e in events:
        if e["kind"] == "fraud_burst":
            bursts.setdefault(e["detail"], []).append(e["msg"]["transaction_id"])
    assert bursts
    for ids in bursts.values():
        assert sum(i in flagged for i in ids) >= len(ids) - 5


def test_almost_all_suspects_are_injected_bursts(sim_run):
    events, _, _, out = sim_run
    burst_ids = {e["msg"]["transaction_id"] for e in events if e["kind"] == "fraud_burst"}
    flagged = [r["transaction_id"] for r in out["transactions"] if r["suspect"]]
    assert sum(i in burst_ids for i in flagged) / len(flagged) > 0.95


def test_parquet_files_are_complete_and_share_one_schema(tmp_path):
    sink = ParquetSink(str(tmp_path), "GW-TEST", batch_size=3, batch_seconds=60)
    proc = EdgeProcessor("GW-TEST", sink.publier, now_fn=lambda: NOW)
    for i in range(7):                       # aucun suspect : raison_suspect toujours vide
        proc.handle(good(transaction_id=f"TX-{i}", timestamp=f"2026-01-05T{10 + i}:00:00Z"))
    proc.handle(good(amount=-1, transaction_id="TX-BAD"))
    sink.flush_all()

    tx_files = sorted(os.listdir(tmp_path / "transactions"))
    assert len(tx_files) == 3                                # 3 + 3 + 1 lignes
    assert not any(f.endswith(".tmp") for f in tx_files)     # jamais de fichier a moitie ecrit
    assert len(set(tx_files)) == len(tx_files)
    for f in tx_files:
        assert pq.read_schema(tmp_path / "transactions" / f).equals(SCHEMAS["transactions"])
    assert sum(pq.read_table(tmp_path / "transactions" / f).num_rows for f in tx_files) == 7
    rej = pq.read_table(tmp_path / "rejets" / os.listdir(tmp_path / "rejets")[0]).to_pylist()
    assert rej[0]["raison_rejet"] == "montant_non_positif"


def test_worker_processes_every_queued_message_in_order():
    proc, out = make()
    q, worker = start_worker(proc)
    for i in range(500):
        q.put(good(transaction_id=f"TX-{i}", timestamp=f"2026-01-05T{i % 24:02d}:00:00Z",
                   card_id=f"CARD-{i:05d}").encode())
    q.join()
    q.put(None)
    worker.join(timeout=5)
    assert not worker.is_alive()
    assert proc.stats["received"] == 500 and proc.stats["validated"] == 500
    assert [r["transaction_id"] for r in out["transactions"]] == [f"TX-{i}" for i in range(500)]