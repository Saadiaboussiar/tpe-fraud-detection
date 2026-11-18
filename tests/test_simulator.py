"""Verifications de l'etape 2 : coherence avec la banque, label absent, rafales detectables, comptages."""
from collections import Counter
from datetime import datetime, timedelta

import pytest

from generator.generate_bank import generate
from simulator.simulate_tpe import build_events

REQUIRED = {"transaction_id", "terminal_id", "merchant_id", "timestamp", "amount",
            "currency", "card_id", "entry_mode", "pin_verified", "auth_response", "mcc"}


@pytest.fixture(scope="module")
def sim():
    _, cartes, commercants, profiles, terminaux = generate(seed=42)
    mcc_of = dict(zip(commercants["merchant_id"], commercants["mcc"]))
    events, gt, fraudes, counts = build_events(cartes, profiles, terminaux, mcc_of, days=21, seed=42)
    return {"events": events, "gt": gt, "fraudes": fraudes, "counts": counts,
            "cartes": cartes, "terminaux": terminaux}


def test_label_never_in_messages(sim):
    for e in sim["events"]:
        assert "is_fraud" not in e["msg"] and "type_fraude" not in e["msg"]


def test_valid_messages_have_all_fields(sim):
    for e in sim["events"]:
        if e["kind"] in ("normal", "legit_unusual", "fraud_burst", "fraud_amount"):
            assert set(e["msg"]) == REQUIRED


def test_ids_match_the_bank(sim):
    cards = set(sim["cartes"]["card_id"])
    terminals = {t["terminal_id"] for t in sim["terminaux"]}
    for e in sim["events"]:
        if "card_id" in e["msg"]:
            assert e["msg"]["card_id"] in cards
        assert e["terminal_id"] in terminals


def test_ids_unique_except_duplicates(sim):
    ids = Counter(e["msg"]["transaction_id"] for e in sim["events"])
    n_dup = sim["counts"]["expected_duplicates"]
    assert sum(v - 1 for v in ids.values()) == n_dup


def test_messages_are_in_chronological_order(sim):
    times = [e["t"] for e in sim["events"]]
    assert times == sorted(times)


def test_counts_are_consistent(sim):
    c = sim["counts"]
    assert c["emitted_expected"] == len(sim["events"])
    assert c["emitted_expected"] == (c["expected_valid_after_edge"] + c["expected_duplicates"]
                                     + c["expected_rejected_by_edge"])
    assert sum(c["by_kind"].values()) == len(sim["events"])


def test_fraud_rate_is_close_to_target(sim):
    assert 0.012 < sim["counts"]["fraud_rate_among_valid"] < 0.03


def test_all_invalid_types_are_injected(sim):
    kinds = sim["counts"]["by_kind"]
    for k in ("invalid_negative_amount", "invalid_missing_field", "invalid_duplicate", "invalid_future_date"):
        assert kinds.get(k, 0) > 0


def test_invalid_messages_are_really_invalid(sim):
    for e in sim["events"]:
        if e["kind"] == "invalid_negative_amount":
            assert e["msg"]["amount"] < 0
        if e["kind"] == "invalid_missing_field":
            assert e["detail"] not in e["msg"]
        if e["kind"] == "invalid_future_date":
            ts = datetime.strptime(e["msg"]["timestamp"], "%Y-%m-%dT%H:%M:%SZ")
            assert ts > e["t"] + timedelta(days=29)


def test_each_burst_has_more_than_5_transactions_within_2_minutes(sim):
    bursts = {}
    for e in sim["events"]:
        if e["kind"] == "fraud_burst":
            bursts.setdefault(e["detail"], []).append(e["t"])
    assert bursts
    for times in bursts.values():
        assert len(times) >= 6
        assert max(times) - min(times) <= timedelta(minutes=2)


def test_confirmed_frauds_match_ground_truth_and_have_a_delay(sim):
    gt_fraud = sim["gt"][sim["gt"]["category"] == "fraud"]
    fr = sim["fraudes"]
    assert set(fr["transaction_id"]) == set(gt_fraud["transaction_id"])
    assert fr["fraud_id"].is_unique
    delays = (fr["date_confirmation"].astype("datetime64[ns]") - fr["date_transaction"].astype("datetime64[ns]")).dt.days
    assert delays.min() >= 3 and delays.max() <= 21


def test_fraud_columns_match_contract(sim):
    assert list(sim["fraudes"].columns) == [
        "fraud_id", "transaction_id", "card_id", "terminal_id", "date_transaction",
        "date_confirmation", "source_confirmation", "montant", "type_fraude"]


def test_amounts_respect_card_ceiling(sim):
    ceiling = dict(zip(sim["cartes"]["card_id"], sim["cartes"]["plafond_transaction"]))
    for e in sim["events"]:
        m = e["msg"]
        if e["kind"] in ("normal", "legit_unusual", "fraud_burst", "fraud_amount"):
            assert 0 < m["amount"] <= ceiling[m["card_id"]]


def test_same_seed_gives_same_messages():
    _, cartes, commercants, profiles, terminaux = generate(seed=3)
    mcc_of = dict(zip(commercants["merchant_id"], commercants["mcc"]))
    a = build_events(cartes, profiles, terminaux, mcc_of, days=7, seed=5)[0]
    b = build_events(cartes, profiles, terminaux, mcc_of, days=7, seed=5)[0]
    assert [e["msg"] for e in a] == [e["msg"] for e in b]