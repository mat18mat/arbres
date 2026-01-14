import json
import sys
from pathlib import Path

# Ajoute le dossier racine du projet au PYTHONPATH
ROOT_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(ROOT_DIR))

from config import get_collection  # noqa: E402


DATA_FILE = ROOT_DIR / "data" / "arbres.json"


def main():
    collection = get_collection()

    if not DATA_FILE.exists():
        raise FileNotFoundError(DATA_FILE)

    with open(DATA_FILE, "r", encoding="utf-8") as f:
        arbres = json.load(f)

    # Nettoyage avant import (TP)
    collection.delete_many({})

    result = collection.insert_many(arbres)
    print(f"{len(result.inserted_ids)} arbres insérés dans MongoDB")


if __name__ == "__main__":
    main()
