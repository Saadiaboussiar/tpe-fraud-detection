"""Verifications de l'etape 1 : pas de doublons de cle, integrite des references, reproductibilite."""
import pandas as pd
import pytest

from generator.generate_bank import generate


@pytest.fixture(scope="module")
def data():
    return generate(seed=42)


def test_no_duplicate_keys(data):
    clients, cartes, commercants, _, terminaux = data
    assert clients["client_id"].is_unique
    assert cartes["card_id"].is_unique
    assert commercants["merchant_id"].is_unique
    assert len({t["terminal_id"] for t in terminaux}) == len(terminaux)


def test_every_card_has_an_existing_client(data):
    clients, cartes, *_ = data
    assert cartes["client_id"].isin(clients["client_id"]).all()


def test_every_client_has_at_least_one_card(data):
    clients, cartes, *_ = data
    assert clients["client_id"].isin(cartes["client_id"]).all()


def test_every_terminal_has_an_existing_merchant(data):
    _, _, commercants, _, terminaux = data
    assert all(t["merchant_id"] in set(commercants["merchant_id"]) for t in terminaux)


def test_profiles_cover_all_cards(data):
    _, cartes, _, profiles, _ = data
    assert set(profiles) == set(cartes["card_id"])


def test_no_habit_columns_in_csv(data):
    _, cartes, *_ = data
    assert not {"mean_amount", "std_amount", "active_hours"} & set(cartes.columns)


def test_columns_match_contract(data):
    clients, cartes, commercants, *_ = data
    assert list(clients.columns) == ["client_id", "ville_residence", "tranche_age", "segment", "date_creation"]
    assert list(cartes.columns) == [
        "card_id", "client_id", "type_carte", "marque", "ville_habituelle", "plafond_transaction",
        "plafond_mensuel", "plafond_sans_contact", "date_emission", "date_expiration", "statut"]
    assert list(commercants.columns) == ["merchant_id", "nom_commercant", "secteur", "mcc", "ville", "statut"]


def test_same_seed_gives_same_data():
    a = generate(seed=1)[1]
    b = generate(seed=1)[1]
    pd.testing.assert_frame_equal(a, b)


def test_cards_fewer_than_clients_is_rejected():
    with pytest.raises(ValueError):
        generate(n_clients=10, n_cards=5)