"""Verifications de l'etape 5 avec un faux client Databricks (aucun reseau, aucun jeton)."""
from types import SimpleNamespace

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from edge.upload import Uploader, collect, count_rows

IOT, BANK = "/Volumes/c/s/iot", "/Volumes/c/s/bank"


class FakeFiles:
    def __init__(self):
        self.store, self.dirs, self.fail_next, self.wrong_size = {}, [], 0, False

    def create_directory(self, path):
        self.dirs.append(path)

    def upload(self, path, fh, overwrite=True):
        if self.fail_next > 0:
            self.fail_next -= 1
            raise RuntimeError("reseau coupe")
        self.store[path] = fh.read()

    def get_metadata(self, path):
        n = len(self.store[path])
        return SimpleNamespace(content_length=n + 1 if self.wrong_size else n)


@pytest.fixture
def env(tmp_path):
    edge = tmp_path / "edge_out"
    (edge / "transactions").mkdir(parents=True)
    (edge / "rejets").mkdir()
    bank = tmp_path / "bank"
    bank.mkdir()
    pq.write_table(pa.table({"a": [1, 2, 3]}), edge / "transactions" / "transactions_001.parquet")
    pq.write_table(pa.table({"a": [1, 2]}), edge / "rejets" / "rejets_001.parquet")
    (edge / "transactions" / "transactions_002.parquet.tmp").write_bytes(b"pas fini")   # ecriture en cours
    (bank / "cartes.csv").write_text("card_id,client_id\nC1,CL1\nC2,CL2\n", encoding="utf-8")
    client = SimpleNamespace(files=FakeFiles())
    up = Uploader(client, tmp_path / "state.json", retries=3, backoff=0, sleep=lambda s: None)
    return SimpleNamespace(edge=edge, bank=bank, client=client, up=up, tmp=tmp_path)


def items(e):
    return collect(e.edge, e.bank, IOT, BANK)


def test_collect_maps_to_the_right_remote_folders_and_ignores_tmp(env):
    remotes = sorted(r for _, r in items(env))
    assert remotes == [f"{BANK}/cartes.csv", f"{IOT}/rejets/rejets_001.parquet",
                       f"{IOT}/transactions/transactions_001.parquet"]


def test_tmp_files_are_never_sent(env):
    env.up.run(items(env))
    assert not any(p.endswith(".tmp") for p in env.client.files.store)
    assert len(env.client.files.store) == 3


def test_sent_content_is_identical_to_local_file(env):
    env.up.run(items(env))
    local = (env.edge / "rejets" / "rejets_001.parquet").read_bytes()
    assert env.client.files.store[f"{IOT}/rejets/rejets_001.parquet"] == local


def test_second_run_sends_nothing(env):
    env.up.run(items(env))
    report = env.up.run(items(env))
    assert report["sent"] == [] and report["skipped_already_sent"] == 3


def test_state_survives_a_restart(env):
    env.up.run(items(env))
    again = Uploader(env.client, env.tmp / "state.json", backoff=0, sleep=lambda s: None)
    assert again.run(items(env))["sent"] == []


def test_new_file_is_sent_alone(env):
    env.up.run(items(env))
    pq.write_table(pa.table({"a": [9]}), env.edge / "transactions" / "transactions_003.parquet")
    report = env.up.run(items(env))
    assert report["sent"] == [f"{IOT}/transactions/transactions_003.parquet"]


def test_modified_csv_is_sent_again(env):
    env.up.run(items(env))
    (env.bank / "cartes.csv").write_text("card_id,client_id\nC1,CL1\nC2,CL2\nC3,CL3\n", encoding="utf-8")
    assert env.up.run(items(env))["sent"] == [f"{BANK}/cartes.csv"]


def test_transient_failure_is_retried(env):
    env.client.files.fail_next = 2                      # 2 echecs puis succes (3 essais)
    report = env.up.run(items(env))
    assert report["failed"] == [] and len(report["sent"]) == 3


def test_permanent_failure_is_reported_and_not_marked_as_sent(env):
    env.client.files.fail_next = 99
    report = env.up.run(items(env))
    assert len(report["failed"]) == 3 and report["sent"] == []
    env.client.files.fail_next = 0
    assert len(env.up.run(items(env))["sent"]) == 3     # le prochain passage reprend


def test_size_mismatch_is_detected(env):
    env.client.files.wrong_size = True
    report = env.up.run(items(env))
    assert len(report["failed"]) == 3
    assert "taille" in report["failed"][0]["error"]


def test_dry_run_sends_nothing_and_records_nothing(env):
    report = env.up.run(items(env), dry_run=True)
    assert len(report["sent"]) == 3
    assert env.client.files.store == {} and not (env.tmp / "state.json").exists()


def test_totals_give_row_counts_per_remote_folder(env):
    env.up.run(items(env))
    t = env.up.totals()
    assert t["transactions"]["rows"] == 3 and t["rejets"]["rows"] == 2 and t["bank"]["rows"] == 2


def test_count_rows(env):
    assert count_rows(env.edge / "transactions" / "transactions_001.parquet") == 3
    assert count_rows(env.bank / "cartes.csv") == 2