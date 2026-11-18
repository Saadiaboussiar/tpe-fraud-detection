"""Generateur de la banque (MVP 1, etape 1).

Produit, de facon reproductible (graine aleatoire) :
  data/bank/clients.csv
  data/bank/cartes.csv
  data/bank/commercants.csv
  data/card_profiles.json   habitudes de chaque carte (lu par le simulateur, hors pipeline)
  data/terminals.json       terminaux simules et leur commercant (le registre CSV viendra au MVP 2)

fraudes_confirmees.csv n'est pas produit ici : il vient du simulateur (etape 2).

Exemple :
  python generator/generate_bank.py
  python generator/generate_bank.py --cities Agadir Casablanca --seed 7
"""

import argparse
import json
import os
from datetime import date, timedelta

import numpy as np
import pandas as pd
from faker import Faker

# secteur -> code MCC (categorie du commercant)
SECTORS = {
    "Supermarche": 5411,
    "Restaurant": 5812,
    "Station-service": 5541,
    "Pharmacie": 5912,
    "Vetements": 5651,
    "Electronique": 5732,
    "Cafe": 5814,
    "Hotel": 7011,
}

AGE_BANDS = ["18-25", "26-35", "36-50", "51-65", "65+"]
BRANDS = ["Visa", "Mastercard"]


def _rand_date(rng, start: date, end: date) -> date:
    return start + timedelta(days=int(rng.integers(0, (end - start).days + 1)))


def generate(n_clients=150, n_cards=200, n_merchants=20, n_terminals=10,
             seed=42, cities=("Agadir",), reference_date=date(2026, 1, 1)):
    """Retourne (clients, cartes, commercants, profils, terminaux)."""
    
    if n_cards < n_clients:
        raise ValueError("n_cards doit etre >= n_clients (chaque client a au moins une carte)")
    if n_terminals < 1 or n_merchants < 1:
        raise ValueError("il faut au moins un commercant et un terminal")

    rng = np.random.default_rng(seed)
    fake = Faker("fr_FR")
    Faker.seed(seed)
    cities = list(cities)

    # ---- clients ----
    clients = pd.DataFrame({
        "client_id": [f"CL-{i:04d}" for i in range(1, n_clients + 1)],
        "ville_residence": rng.choice(cities, n_clients),
        "tranche_age": rng.choice(AGE_BANDS, n_clients, p=[0.15, 0.30, 0.30, 0.18, 0.07]),
        "segment": rng.choice(["standard", "premium"], n_clients, p=[0.8, 0.2]),
        "date_creation": [_rand_date(rng, date(2015, 1, 1), reference_date - timedelta(days=400)).isoformat()
                          for _ in range(n_clients)],
    })

    # ---- cartes : chaque client en a au moins une, le reste est reparti au hasard ----
    owners = list(clients["client_id"])
    owners += list(rng.choice(clients["client_id"], n_cards - n_clients))
    rng.shuffle(owners)
    seg_of = dict(zip(clients["client_id"], clients["segment"]))
    city_of = dict(zip(clients["client_id"], clients["ville_residence"]))

    rows, profiles = [], {}
    for i, cid in enumerate(owners, start=1):
        card_id = f"CARD-{i:05d}"
        premium = seg_of[cid] == "premium"
        plafond_tx = 20000.0 if premium else 5000.0
        issued = _rand_date(rng, reference_date - timedelta(days=1400), reference_date - timedelta(days=200))
        rows.append({
            "card_id": card_id,
            "client_id": cid,
            "type_carte": rng.choice(["debit", "credit"], p=[0.75, 0.25]),
            "marque": rng.choice(BRANDS),
            "ville_habituelle": city_of[cid],
            "plafond_transaction": plafond_tx,
            "plafond_mensuel": plafond_tx * 5,
            "plafond_sans_contact": 300.0,
            "date_emission": issued.isoformat(),
            "date_expiration": (issued + timedelta(days=4 * 365)).isoformat(),
            "statut": "Active" if rng.random() < 0.97 else "Bloquee",
        })
        # habitudes : parametres du generateur, PAS des colonnes du CSV
        mean = float(rng.uniform(150, 900) if premium else rng.uniform(40, 350))
        start_h = int(rng.integers(8, 14))
        profiles[card_id] = {
            "mean_amount": round(mean, 2),
            "std_amount": round(mean * float(rng.uniform(0.3, 0.6)), 2),
            "active_hours": [start_h, int(min(23, start_h + rng.integers(6, 11)))],
            "tx_per_day": round(float(rng.uniform(0.3, 2.0)), 2),
        }
    cartes = pd.DataFrame(rows)

    # ---- commercants ----
    sectors = rng.choice(list(SECTORS), n_merchants)
    commercants = pd.DataFrame({
        "merchant_id": [f"M-{i:03d}" for i in range(1, n_merchants + 1)],
        "nom_commercant": [f"{s} {fake.last_name()}" for s in sectors],
        "secteur": sectors,
        "mcc": [SECTORS[s] for s in sectors],
        "ville": rng.choice(cities, n_merchants),
        "statut": "Actif",
    })

    # ---- terminaux simules (chacun appartient a un commercant) ----
    terminaux = [
        {"terminal_id": f"TPE-{i:03d}",
         "merchant_id": commercants["merchant_id"].iloc[(i - 1) % n_merchants],
         "ville": commercants["ville"].iloc[(i - 1) % n_merchants]}
        for i in range(1, n_terminals + 1)
    ]
    return clients, cartes, commercants, profiles, terminaux


def main():
    p = argparse.ArgumentParser(description="Generateur de la banque (donnees synthetiques)")
    p.add_argument("--clients", type=int, default=150)
    p.add_argument("--cards", type=int, default=200)
    p.add_argument("--merchants", type=int, default=20)
    p.add_argument("--terminals", type=int, default=10)
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--cities", nargs="+", default=["Agadir"])
    p.add_argument("--out", default="data/bank", help="dossier des CSV")
    p.add_argument("--aux", default="data", help="dossier des fichiers hors pipeline (profils, terminaux)")
    a = p.parse_args()

    clients, cartes, commercants, profiles, terminaux = generate(
        a.clients, a.cards, a.merchants, a.terminals, a.seed, a.cities)

    os.makedirs(a.out, exist_ok=True)
    os.makedirs(a.aux, exist_ok=True)
    clients.to_csv(os.path.join(a.out, "clients.csv"), index=False, encoding="utf-8")
    cartes.to_csv(os.path.join(a.out, "cartes.csv"), index=False, encoding="utf-8")
    commercants.to_csv(os.path.join(a.out, "commercants.csv"), index=False, encoding="utf-8")
    with open(os.path.join(a.aux, "card_profiles.json"), "w", encoding="utf-8") as f:
        json.dump(profiles, f, indent=2)
    with open(os.path.join(a.aux, "terminals.json"), "w", encoding="utf-8") as f:
        json.dump(terminaux, f, indent=2)

    print(f"clients={len(clients)} cartes={len(cartes)} commercants={len(commercants)} "
          f"terminaux={len(terminaux)} seed={a.seed}")
    print(f"CSV dans {a.out}/, profils et terminaux dans {a.aux}/")


if __name__ == "__main__":
    main()