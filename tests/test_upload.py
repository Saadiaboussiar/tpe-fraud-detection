import os
import pandas as pd
from dotenv import load_dotenv
from databricks.sdk import WorkspaceClient

load_dotenv()
iot_vol = os.environ["DATABRICKS_VOLUME_IOT"]
bank_vol = os.environ["DATABRICKS_VOLUME_BANK"]

# petit fichier de test local
os.makedirs("data", exist_ok=True)
pd.DataFrame({"id": [1, 2, 3], "txt": ["a", "b", "c"]}).to_parquet("data/test.parquet")

w = WorkspaceClient(host=os.environ["DATABRICKS_HOST"],
                    token=os.environ["DATABRICKS_TOKEN"])

# sous-dossiers de chaque Volume (sans erreur s'ils existent déjà)
for d in [f"{iot_vol}/transactions", f"{iot_vol}/rejets", f"{iot_vol}/test",
          f"{bank_vol}/test"]:
    w.files.create_directory(d)

# envoi du fichier de test vers les deux Volumes
for dest in [f"{iot_vol}/test/test.parquet", f"{bank_vol}/test/test.parquet"]:
    with open("data/test.parquet", "rb") as f:
        w.files.upload(dest, f, overwrite=True)

# vérification : liste des fichiers présents
for folder in [f"{iot_vol}/test", f"{bank_vol}/test"]:
    print(folder, [x.path for x in w.files.list_directory_contents(folder)])