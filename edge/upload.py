"""Script d'envoi vers Databricks (MVP 1, etape 5) : passage officiel de l'Edge au Cloud.

Envoie dans les Volumes Databricks, et uniquement les fichiers TERMINES (jamais les .tmp) :
  data/edge_out/transactions/*.parquet -> <VOLUME_IOT>/transactions/
  data/edge_out/rejets/*.parquet       -> <VOLUME_IOT>/rejets/
  data/bank/*.csv                      -> <VOLUME_BANK>/

Pour chaque fichier : envoi, verification de la taille distante, reessais en cas d'echec,
puis enregistrement dans data/upload_state.json ("deja envoye"). Relancer le script
n'envoie donc rien de deja envoye (un CSV modifie est renvoye).

Les identifiants viennent du fichier .env (jamais publie) :
  DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_VOLUME_IOT, DATABRICKS_VOLUME_BANK

  python edge/upload.py --dry-run     # liste ce qui serait envoye
  python edge/upload.py               # un passage
  python edge/upload.py --watch 30    # surveille le dossier toutes les 30 s (Ctrl+C pour arreter)
"""
import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath

import pyarrow.parquet as pq


def collect(edge_out, bank_dir, iot_vol, bank_vol):
    """Liste (fichier local, chemin distant). Les .tmp ne correspondent a aucun motif."""
    items = []
    for family in ("transactions", "rejets"):
        folder = Path(edge_out) / family
        if folder.is_dir():
            items += [(f, f"{iot_vol}/{family}/{f.name}") for f in sorted(folder.glob("*.parquet"))]
    bank = Path(bank_dir)
    if bank.is_dir():
        items += [(f, f"{bank_vol}/{f.name}") for f in sorted(bank.glob("*.csv"))]
    return items


def count_rows(path):
    path = Path(path)
    if path.suffix == ".parquet":
        return pq.ParquetFile(path).metadata.num_rows
    with open(path, encoding="utf-8") as f:
        return max(0, sum(1 for _ in f) - 1)          # sans la ligne d'en-tete


class Uploader:
    def __init__(self, client, state_path, retries=3, backoff=2.0, sleep=time.sleep):
        self.client, self.state_path = client, Path(state_path)
        self.retries, self.backoff, self.sleep = retries, backoff, sleep
        self.state = {}
        if self.state_path.exists():
            self.state = json.loads(self.state_path.read_text(encoding="utf-8"))

    def _save(self):
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        self.state_path.write_text(json.dumps(self.state, indent=2), encoding="utf-8")

    @staticmethod
    def _key(path):
        return Path(path).as_posix()

    def is_sent(self, local):
        entry = self.state.get(self._key(local))
        st = Path(local).stat()
        return bool(entry) and entry["size"] == st.st_size and entry["mtime"] == int(st.st_mtime)

    def _send(self, local, remote):
        last = None
        for attempt in range(1, self.retries + 1):
            try:
                with open(local, "rb") as fh:
                    self.client.files.upload(remote, fh, overwrite=True)
                size = os.path.getsize(local)
                remote_size = self.client.files.get_metadata(remote).content_length
                if remote_size != size:
                    raise OSError(f"taille distante {remote_size} != taille locale {size}")
                return
            except Exception as exc:               # reessai, puis on remonte l'erreur
                last = exc
                if attempt < self.retries:
                    self.sleep(self.backoff * 2 ** (attempt - 1))
        raise last

    def run(self, items, dry_run=False):
        sent, skipped, failed = [], 0, []
        made_dirs = set()
        for local, remote in items:
            if self.is_sent(local):
                skipped += 1
                continue
            if dry_run:
                sent.append(remote)
                continue
            parent = str(PurePosixPath(remote).parent)
            if parent not in made_dirs:
                try:
                    self.client.files.create_directory(parent)
                except Exception:
                    pass                           # existe deja, ou sera cree par l'envoi
                made_dirs.add(parent)
            try:
                self._send(local, remote)
            except Exception as exc:
                failed.append({"file": str(local), "error": str(exc)})
                continue
            st = Path(local).stat()
            self.state[self._key(local)] = {
                "remote": remote, "size": st.st_size, "mtime": int(st.st_mtime),
                "rows": count_rows(local),
                "sent_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")}
            self._save()
            sent.append(remote)
        return {"sent": sent, "skipped_already_sent": skipped, "failed": failed}

    def totals(self):
        """Lignes et fichiers envoyes depuis le debut, par dossier distant : les comptages 'avant envoi'."""
        out = {}
        for entry in self.state.values():
            folder = PurePosixPath(entry["remote"]).parent.name
            t = out.setdefault(folder, {"files": 0, "rows": 0, "bytes": 0})
            t["files"] += 1
            t["rows"] += entry["rows"]
            t["bytes"] += entry["size"]
        return out


def main():
    p = argparse.ArgumentParser(description="Envoi des fichiers termines vers les Volumes Databricks")
    p.add_argument("--edge-out", default="data/edge_out")
    p.add_argument("--bank-dir", default="data/bank")
    p.add_argument("--state", default="data/upload_state.json")
    p.add_argument("--report", default="data/upload_databricks_report.json")
    p.add_argument("--retries", type=int, default=3)
    p.add_argument("--watch", type=float, default=0.0, help="repete toutes les N secondes (0 = un seul passage)")
    p.add_argument("--dry-run", action="store_true", help="liste les fichiers sans rien envoyer")
    a = p.parse_args()

    from dotenv import load_dotenv
    load_dotenv()
    env = {k: os.getenv(k) for k in ("DATABRICKS_HOST", "DATABRICKS_TOKEN",
                                     "DATABRICKS_VOLUME_IOT", "DATABRICKS_VOLUME_BANK")}
    missing = [k for k, v in env.items() if not v]
    if missing:
        sys.exit(f"Variables manquantes dans .env : {', '.join(missing)}")
    iot_vol, bank_vol = env["DATABRICKS_VOLUME_IOT"].rstrip("/"), env["DATABRICKS_VOLUME_BANK"].rstrip("/")

    if a.dry_run:
        client = None
    else:
        from databricks.sdk import WorkspaceClient
        client = WorkspaceClient(host=env["DATABRICKS_HOST"], token=env["DATABRICKS_TOKEN"])
    up = Uploader(client, a.state, retries=a.retries)

    exit_code = 0
    try:
        while True:
            report = up.run(collect(a.edge_out, a.bank_dir, iot_vol, bank_vol), dry_run=a.dry_run)
            verb = "a envoyer" if a.dry_run else "envoyes"
            print(f"{len(report['sent'])} fichier(s) {verb}, {report['skipped_already_sent']} deja envoye(s), "
                  f"{len(report['failed'])} echec(s)")
            for r in report["sent"]:
                print("  ->", r)
            for f in report["failed"]:
                print("  ECHEC", f["file"], "-", f["error"])
                exit_code = 1
            if not a.dry_run:
                report["totals_since_start"] = up.totals()
                Path(a.report).parent.mkdir(parents=True, exist_ok=True)
                Path(a.report).write_text(json.dumps(report, indent=2), encoding="utf-8")
                print("total depuis le debut :", up.totals())
            if not a.watch:
                break
            time.sleep(a.watch)
    except KeyboardInterrupt:
        print("arret manuel")
    sys.exit(exit_code)


if __name__ == "__main__":
    main()