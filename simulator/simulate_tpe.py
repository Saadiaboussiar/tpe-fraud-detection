"""Simulateur de TPE (MVP 1, etape 2).

Lit les fichiers du generateur de la banque (cartes, commercants, profils, terminaux),
fabrique des transactions coherentes avec les habitudes de chaque carte, injecte des fraudes
et des messages invalides, puis les publie en MQTT sur  tpe/<terminal_id>/transactions.

Fraudes injectees :   rafale (>5 transactions sur une carte en 2 minutes)  et  montant anormal.
Messages invalides :  montant negatif, champ manquant, doublon, date dans le futur.
Cas legitimes inhabituels : gros achats (label 0), pour que le modele ait quelque chose a apprendre.

Sorties :
  data/sim/ground_truth.csv        verite terrain (HORS chaine, jamais envoyee)
  data/sim/counts.json             comptages d'injection et d'emission (preuve de l'etape)
  data/bank/fraudes_confirmees.csv fraudes confirmees avec un delai (mode accelere seulement)

Le label is_fraud ne figure JAMAIS dans les messages.

Exemples :
  python simulator/simulate_tpe.py --dry-run                      # sans Mosquitto, ecrit un fichier JSONL
  python simulator/simulate_tpe.py                                # mode accelere, publie sur Mosquitto
  python simulator/simulate_tpe.py --mode realtime --days 1 --speed 600   # demonstration
"""
import argparse
import copy
import json
import os
import time
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

MISSING_CHOICES = ["card_id", "amount", "timestamp", "merchant_id"]
INVALID_KINDS = ["negative_amount", "missing_field", "duplicate", "future_date"]
SANS_CONTACT_LIMIT = 300.0


def _iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def _day_start(day0, index):
    return datetime.combine(day0, datetime.min.time()) + timedelta(days=index)


def _rand_time(rng, day0, day_index, hours):
    h = int(rng.integers(hours[0], hours[1] + 1))
    return _day_start(day0, day_index) + timedelta(
        hours=h, minutes=int(rng.integers(0, 60)), seconds=int(rng.integers(0, 60)))


def _entry_mode(rng, amount):
    r = rng.random()
    if r < 0.55:
        mode, pin = "puce", True
    elif r < 0.90:
        mode, pin = "sans_contact", False
    elif r < 0.95:
        mode, pin = "bande_magnetique", bool(rng.random() < 0.5)
    else:
        mode, pin = "saisie_manuelle", False
    if mode == "sans_contact" and amount > SANS_CONTACT_LIMIT:
        mode, pin = "puce", True
    return mode, pin


def _tx(card_id, terminal, mcc_of, t, amount, entry_mode, pin):
    return {
        "transaction_id": None,
        "terminal_id": terminal["terminal_id"],
        "merchant_id": terminal["merchant_id"],
        "timestamp": _iso(t),
        "amount": round(float(amount), 2),
        "currency": "MAD",
        "card_id": card_id,
        "entry_mode": entry_mode,
        "pin_verified": bool(pin),
        "auth_response": "approuvee",
        "mcc": int(mcc_of[terminal["merchant_id"]]),
    }


def build_events(cards, profiles, terminals, mcc_of, start=date(2026, 1, 5), days=35,
                 fraud_rate=0.02, invalid_rate=0.02, legit_rate=0.005,
                 confirm_days=(3, 21), seed=42, id_prefix="TX"):
    """Fonction pure : retourne (events, ground_truth, fraudes_confirmees, counts)."""
    rng = np.random.default_rng(seed)
    active = cards[cards["statut"] == "Active"].sort_values("card_id").reset_index(drop=True)
    plafond = dict(zip(cards["card_id"], cards["plafond_transaction"]))
    events = []

    def add(t, terminal, tx, kind, detail="", dup_of=None):
        events.append({"t": t, "terminal_id": terminal["terminal_id"], "tx": tx,
                       "kind": kind, "detail": detail, "dup_of": dup_of})

    def pick_terminal():
        return terminals[int(rng.integers(len(terminals)))]

    def pick_card():
        return active.iloc[int(rng.integers(len(active)))]["card_id"]

    # ---- 1. transactions normales (et quelques cas legitimes inhabituels) ----
    for card_id in active["card_id"]:
        p = profiles[card_id]
        for d in range(days):
            for _ in range(int(rng.poisson(p["tx_per_day"]))):
                terminal = pick_terminal()
                t = _rand_time(rng, start, d, p["active_hours"])
                legit_hard = rng.random() < legit_rate
                if legit_hard:
                    amount = p["mean_amount"] * rng.uniform(3, 5)
                else:
                    amount = max(1.0, rng.normal(p["mean_amount"], p["std_amount"]))
                amount = min(amount, plafond[card_id] * 0.95)
                mode, pin = _entry_mode(rng, amount)
                add(t, terminal, _tx(card_id, terminal, mcc_of, t, amount, mode, pin),
                    "legit_unusual" if legit_hard else "normal")

    n_base = len(events)

    # ---- 2. fraudes : moitie rafales, moitie montants anormaux ----
    target = int(round(fraud_rate * n_base / (1 - fraud_rate)))
    burst_target = target // 2
    burst_tx, burst_n = 0, 0
    while burst_tx < burst_target:
        card_id = pick_card()
        p = profiles[card_id]
        terminal = pick_terminal()
        t = _rand_time(rng, start, int(rng.integers(0, days)), p["active_hours"])
        k = int(rng.integers(6, 11))               # 6 a 10 transactions
        burst_n += 1
        for j in range(k):
            if j:
                t += timedelta(seconds=int(rng.integers(4, 13)))   # 9 ecarts max = 108 s < 2 min
            amount = min(max(5.0, p["mean_amount"] * rng.uniform(0.5, 2.0)), plafond[card_id] * 0.95)
            mode = str(rng.choice(["sans_contact", "bande_magnetique", "saisie_manuelle"]))
            if mode == "sans_contact" and amount > SANS_CONTACT_LIMIT:
                mode = "bande_magnetique"
            add(t, terminal, _tx(card_id, terminal, mcc_of, t, amount, mode, False),
                "fraud_burst", detail=f"burst_{burst_n}")
        burst_tx += k

    for _ in range(max(0, target - burst_tx)):
        card_id = pick_card()
        p = profiles[card_id]
        terminal = pick_terminal()
        t = _day_start(start, int(rng.integers(0, days))) + timedelta(
            hours=int(rng.integers(1, 6)), minutes=int(rng.integers(0, 60)), seconds=int(rng.integers(0, 60)))
        amount = min(p["mean_amount"] * rng.uniform(8, 20), plafond[card_id] * 0.98)
        mode = str(rng.choice(["saisie_manuelle", "bande_magnetique"]))
        add(t, terminal, _tx(card_id, terminal, mcc_of, t, amount, mode, False), "fraud_amount")

    # ---- 3. messages invalides, repartis entre les 4 types ----
    n_valid = len(events)
    n_invalid = int(round(invalid_rate * n_valid))
    per_kind = {k: n_invalid // 4 + (1 if i < n_invalid % 4 else 0) for i, k in enumerate(INVALID_KINDS)}
    normal_idx = [i for i, e in enumerate(events) if e["kind"] == "normal"]
    for kind, n in per_kind.items():
        for _ in range(n):
            if kind == "duplicate":
                src = int(normal_idx[int(rng.integers(len(normal_idx)))])
                add(events[src]["t"] + timedelta(seconds=int(rng.integers(1, 6))),
                    {"terminal_id": events[src]["terminal_id"]}, None, "invalid_duplicate", dup_of=src)
                continue
            card_id = pick_card()
            p = profiles[card_id]
            terminal = pick_terminal()
            t = _rand_time(rng, start, int(rng.integers(0, days)), p["active_hours"])
            amount = min(max(1.0, rng.normal(p["mean_amount"], p["std_amount"])), plafond[card_id] * 0.95)
            mode, pin = _entry_mode(rng, amount)
            detail = str(rng.choice(MISSING_CHOICES)) if kind == "missing_field" else ""
            add(t, terminal, _tx(card_id, terminal, mcc_of, t, amount, mode, pin),
                f"invalid_{kind}", detail=detail)

    # ---- 4. tri chronologique, identifiants, application des defauts ----
    order = sorted(range(len(events)), key=lambda i: (events[i]["t"], i))
    final, n_id = [], 0
    for i in order:
        e = events[i]
        if e["dup_of"] is not None:
            msg = copy.deepcopy(events[e["dup_of"]]["clean"])
        else:
            n_id += 1
            e["tx"]["transaction_id"] = f"{id_prefix}-{n_id:07d}"
            e["clean"] = copy.deepcopy(e["tx"])
            msg = e["tx"]
            if e["kind"] == "invalid_negative_amount":
                msg["amount"] = -abs(msg["amount"])
            elif e["kind"] == "invalid_missing_field":
                del msg[e["detail"]]
            elif e["kind"] == "invalid_future_date":
                msg["timestamp"] = _iso(e["t"] + timedelta(days=30))
        e["msg"] = msg
        final.append(e)

    # ---- 5. verite terrain, fraudes confirmees, comptages ----
    gt, fraudes = [], []
    for e in final:
        if e["kind"] == "normal":
            continue
        c = e["clean"] if "clean" in e else events[e["dup_of"]]["clean"]
        category = ("fraud" if e["kind"].startswith("fraud") else
                    "legit_hard" if e["kind"] == "legit_unusual" else "invalid")
        gt.append({"transaction_id": e["msg"]["transaction_id"], "category": category,
                   "injected_type": e["kind"], "detail": e["detail"], "card_id": c["card_id"],
                   "terminal_id": c["terminal_id"], "timestamp": _iso(e["t"]), "amount": c["amount"]})
        if category == "fraud":
            delay = int(rng.integers(confirm_days[0], confirm_days[1] + 1))
            fraudes.append({
                "fraud_id": f"F-{len(fraudes) + 1:05d}",
                "transaction_id": e["msg"]["transaction_id"],
                "card_id": c["card_id"], "terminal_id": c["terminal_id"],
                "date_transaction": e["t"].date().isoformat(),
                "date_confirmation": (e["t"].date() + timedelta(days=delay)).isoformat(),
                "source_confirmation": str(rng.choice(["chargeback", "signalement_client"])),
                "montant": c["amount"],
                "type_fraude": "rafale" if e["kind"] == "fraud_burst" else "montant_anormal",
            })

    kinds = pd.Series([e["kind"] for e in final]).value_counts().to_dict()
    n_dup = kinds.get("invalid_duplicate", 0)
    n_rej = sum(v for k, v in kinds.items() if k.startswith("invalid_") and k != "invalid_duplicate")
    n_fraud = kinds.get("fraud_burst", 0) + kinds.get("fraud_amount", 0)
    n_ok = len(final) - n_dup - n_rej
    counts = {
        "emitted_expected": len(final),
        "by_kind": {k: int(v) for k, v in sorted(kinds.items())},
        "expected_duplicates": int(n_dup),
        "expected_rejected_by_edge": int(n_rej),
        "expected_valid_after_edge": int(n_ok),
        "fraud_transactions": int(n_fraud),
        "fraud_rate_among_valid": round(n_fraud / n_ok, 4) if n_ok else 0.0,
        "params": {"start": start.isoformat(), "days": days, "fraud_rate": fraud_rate,
                   "invalid_rate": invalid_rate, "legit_rate": legit_rate,
                   "confirm_days": list(confirm_days), "seed": seed, "id_prefix": id_prefix},
    }
    return final, pd.DataFrame(gt), pd.DataFrame(fraudes), counts


def load_inputs(bank_dir, aux_dir):
    cards = pd.read_csv(os.path.join(bank_dir, "cartes.csv"))
    merchants = pd.read_csv(os.path.join(bank_dir, "commercants.csv"))
    with open(os.path.join(aux_dir, "card_profiles.json"), encoding="utf-8") as f:
        profiles = json.load(f)
    with open(os.path.join(aux_dir, "terminals.json"), encoding="utf-8") as f:
        terminals = json.load(f)
    return cards, profiles, terminals, dict(zip(merchants["merchant_id"], merchants["mcc"]))


def _payload_and_topic(e):
    return f"tpe/{e['terminal_id']}/transactions", json.dumps(e["msg"], ensure_ascii=False)


def _retime(e, target):
    """Mode temps reel : l'horodatage du message devient l'heure d'emission (compressee)."""
    if "timestamp" in e["msg"]:
        shift = timedelta(days=30) if e["kind"] == "invalid_future_date" else timedelta(0)
        e["msg"]["timestamp"] = _iso(target + shift)


def run(events, mode, speed, host, port, dry_run_path=None):
    """Publie les evenements. Retourne le nombre de messages emis."""
    t0 = events[0]["t"]
    wall0 = datetime.now(timezone.utc).replace(tzinfo=None)
    mono0 = time.monotonic()
    client, infos, out = None, [], None

    if dry_run_path:
        os.makedirs(os.path.dirname(dry_run_path) or ".", exist_ok=True)
        out = open(dry_run_path, "w", encoding="utf-8")
    else:
        import paho.mqtt.client as mqtt
        try:
            client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="tpe-simulator")
        except AttributeError:          # paho-mqtt 1.x
            client = mqtt.Client(client_id="tpe-simulator")
        try:
            client.connect(host, port, keepalive=60)
        except OSError as exc:
            raise SystemExit(f"Mosquitto injoignable sur {host}:{port} ({exc}). "
                             f"Lancez 'docker compose up -d' ou utilisez --dry-run.")
        client.loop_start()

    emitted = 0
    try:
        for e in events:
            if mode == "realtime":
                offset = (e["t"] - t0).total_seconds() / speed
                delay = mono0 + offset - time.monotonic()
                if delay > 0:
                    time.sleep(delay)
                _retime(e, wall0 + timedelta(seconds=offset))
            topic, payload = _payload_and_topic(e)
            if out:
                out.write(json.dumps({"topic": topic, "payload": json.loads(payload)}, ensure_ascii=False) + "\n")
            else:
                infos.append(client.publish(topic, payload, qos=1))
            emitted += 1
        for info in infos:
            info.wait_for_publish(timeout=30)
    finally:
        if out:
            out.close()
        if client:
            client.loop_stop()
            client.disconnect()
    return emitted


def main():
    p = argparse.ArgumentParser(description="Simulateur de TPE (donnees synthetiques)")
    p.add_argument("--mode", choices=["accelerated", "realtime"], default="accelerated")
    p.add_argument("--days", type=int, default=35, help="duree simulee en jours")
    p.add_argument("--start", default="2026-01-05", help="premier jour simule (AAAA-MM-JJ)")
    p.add_argument("--fraud-rate", type=float, default=0.02)
    p.add_argument("--invalid-rate", type=float, default=0.02)
    p.add_argument("--legit-rate", type=float, default=0.005)
    p.add_argument("--confirm-min", type=int, default=3)
    p.add_argument("--confirm-max", type=int, default=21)
    p.add_argument("--speed", type=float, default=60.0, help="temps reel : 60 = 1 min simulee par seconde")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--id-prefix", default=None, help="prefixe des transaction_id (TX, ou TXR en temps reel)")
    p.add_argument("--host", default=os.getenv("MQTT_HOST", "localhost"))
    p.add_argument("--port", type=int, default=int(os.getenv("MQTT_PORT", "1883")))
    p.add_argument("--bank-dir", default="data/bank")
    p.add_argument("--aux-dir", default="data")
    p.add_argument("--sim-dir", default="data/sim")
    p.add_argument("--dry-run", action="store_true", help="ecrit les messages dans un fichier JSONL au lieu de MQTT")
    a = p.parse_args()

    realtime = a.mode == "realtime"
    prefix = a.id_prefix or ("TXR" if realtime else "TX")   # evite les collisions d'identifiants entre les deux modes
    suffix = "_realtime" if realtime else ""

    cards, profiles, terminals, mcc_of = load_inputs(a.bank_dir, a.aux_dir)
    events, gt, fraudes, counts = build_events(
        cards, profiles, terminals, mcc_of, date.fromisoformat(a.start), a.days,
        a.fraud_rate, a.invalid_rate, a.legit_rate, (a.confirm_min, a.confirm_max), a.seed, prefix)

    os.makedirs(a.sim_dir, exist_ok=True)
    gt.to_csv(os.path.join(a.sim_dir, f"ground_truth{suffix}.csv"), index=False, encoding="utf-8")
    if not realtime:      # le temps reel ne doit pas ecraser les fraudes confirmees du lot accelere
        os.makedirs(a.bank_dir, exist_ok=True)
        fraudes.to_csv(os.path.join(a.bank_dir, "fraudes_confirmees.csv"), index=False, encoding="utf-8")

    dry = os.path.join(a.sim_dir, f"messages{suffix}.jsonl") if a.dry_run else None
    emitted = run(events, a.mode, a.speed, a.host, a.port, dry)
    counts["emitted"] = emitted
    with open(os.path.join(a.sim_dir, f"counts{suffix}.json"), "w", encoding="utf-8") as f:
        json.dump(counts, f, indent=2, ensure_ascii=False)

    print(f"messages emis : {emitted}  (attendus : {counts['emitted_expected']})")
    print(f"fraudes : {counts['fraud_transactions']}  |  doublons : {counts['expected_duplicates']}  "
          f"|  rejets attendus a l'Edge : {counts['expected_rejected_by_edge']}  "
          f"|  valides attendus : {counts['expected_valid_after_edge']}")
    print(f"comptages dans {a.sim_dir}/counts{suffix}.json")


if __name__ == "__main__":
    main()