import json
import os
from pathlib import Path

import pandas as pd


RAW_DIR = Path("data-raw")
OUT_DIR = Path("data")
OUT_FILE = OUT_DIR / "arbres.json"


def guess_lat_lon(row: pd.Series) -> tuple[float | None, float | None]:
    """
    Essaie de trouver latitude/longitude dans différentes colonnes possibles.
    Retourne (lat, lon) ou (None, None).
    """
    # Cas fréquent: "geo_point_2d" = "48.8566, 2.3522"
    for col in ["geo_point_2d", "geo_point", "geopoint", "coordonnees", "coordinates"]:
        if col in row and pd.notna(row[col]):
            val = str(row[col]).strip()
            if "," in val:
                parts = [p.strip() for p in val.split(",")]
                if len(parts) >= 2:
                    try:
                        a = float(parts[0])
                        b = float(parts[1])
                        # Heuristique: si a ressemble à une latitude (≈ 48) et b à une longitude (≈ 2)
                        # sinon on inverse.
                        if -90 <= a <= 90 and -180 <= b <= 180:
                            return a, b
                        if -90 <= b <= 90 and -180 <= a <= 180:
                            return b, a
                    except ValueError:
                        pass

    # Autres cas: colonnes séparées
    lat_cols = ["lat", "latitude", "y_lat"]
    lon_cols = ["lon", "lng", "longitude", "x_lon"]
    lat = None
    lon = None
    for c in lat_cols:
        if c in row and pd.notna(row[c]):
            try:
                lat = float(row[c])
                break
            except ValueError:
                pass
    for c in lon_cols:
        if c in row and pd.notna(row[c]):
            try:
                lon = float(row[c])
                break
            except ValueError:
                pass

    return lat, lon


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df.columns = [str(c).strip().lower() for c in df.columns]
    return df


def to_common_format(df: pd.DataFrame, source: str) -> list[dict]:
    """
    Transforme un DataFrame (peu importe son schéma) en liste de dicts homogènes.
    On garde le max d'infos utiles + geo si dispo.
    """
    df = normalize_columns(df)

    # champs "communs" possibles
    possible_city = ["commune", "ville", "arrondissement", "localite"]
    possible_addr = ["adresse", "address", "lieu", "libellefrancais", "libelléfrancais"]
    possible_species = ["espece", "espèce", "species", "libellefrancais", "libelléfrancais", "genre", "variete", "variété"]
    possible_id = ["id", "objectid", "gid", "identifiant", "id_arbre", "idbase"]

    out = []
    for _, row in df.iterrows():
        obj = {
            "source": source,
            "id_source": None,
            "commune": None,
            "adresse": None,
            "espece": None,
            "hauteur_m": None,
            "circonference_cm": None,
            "remarquable": None,
            "geo": {"lat": None, "lon": None},
            "raw": {},  # on garde aussi les champs originaux (utile pour le TP)
        }

        # id_source
        for c in possible_id:
            c = c.lower()
            if c in df.columns and pd.notna(row.get(c)):
                obj["id_source"] = str(row.get(c))
                break

        # commune
        for c in possible_city:
            c = c.lower()
            if c in df.columns and pd.notna(row.get(c)):
                obj["commune"] = str(row.get(c))
                break

        # adresse
        for c in possible_addr:
            c = c.lower()
            if c in df.columns and pd.notna(row.get(c)):
                obj["adresse"] = str(row.get(c))
                break

        # espece (on concatène si possible)
        parts = []
        for c in possible_species:
            c = c.lower()
            if c in df.columns and pd.notna(row.get(c)):
                val = str(row.get(c)).strip()
                if val and val not in parts:
                    parts.append(val)
        if parts:
            obj["espece"] = " / ".join(parts[:3])

        # hauteur / circonférence si on les trouve
        for c in ["hauteur", "hauteur_m", "hauteur(en m)", "hauteur_metre", "hauteurmetre"]:
            c = c.lower()
            if c in df.columns and pd.notna(row.get(c)):
                try:
                    obj["hauteur_m"] = float(str(row.get(c)).replace(",", "."))
                    break
                except ValueError:
                    pass

        for c in ["circonference", "circonférence", "circonference_cm", "circonférence_cm"]:
            c = c.lower()
            if c in df.columns and pd.notna(row.get(c)):
                try:
                    obj["circonference_cm"] = float(str(row.get(c)).replace(",", "."))
                    break
                except ValueError:
                    pass

        # remarquable (si la colonne existe)
        for c in ["remarquable", "est_remarquable", "classement", "statut"]:
            c = c.lower()
            if c in df.columns and pd.notna(row.get(c)):
                v = str(row.get(c)).strip().lower()
                obj["remarquable"] = v in ["1", "true", "oui", "yes", "vrai"]
                break

        # geo
        lat, lon = guess_lat_lon(row)
        obj["geo"]["lat"] = lat
        obj["geo"]["lon"] = lon

        # raw (tout le reste)
        obj["raw"] = {k: (None if pd.isna(v) else v) for k, v in row.to_dict().items()}

        out.append(obj)

    return out


def main():
    paris_file = RAW_DIR / "les-arbres.csv"
    hds_file = RAW_DIR / "arbres-remarquables-du-territoire-des-hauts-de-seine-hors-proprietes-privees.csv"


    if not paris_file.exists():
        raise FileNotFoundError(f"Fichier manquant: {paris_file}")
    if not hds_file.exists():
        raise FileNotFoundError(f"Fichier manquant: {hds_file}")

    # Lecture CSV (séparateur automatique)
    df_paris = pd.read_csv(paris_file, sep=None, engine="python")
    df_hds = pd.read_csv(hds_file, sep=None, engine="python")

    # Important: Paris -> on ne garde que remarquables si une colonne le permet
    tmp = normalize_columns(df_paris)
    if "remarquable" in tmp.columns:
        df_paris = tmp[tmp["remarquable"].astype(str).str.lower().isin(["1", "true", "oui", "yes"])]

    OUT_DIR.mkdir(exist_ok=True)

    all_records = []
    all_records.extend(to_common_format(df_paris, "paris"))
    all_records.extend(to_common_format(df_hds, "hauts-de-seine"))

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_records, f, ensure_ascii=False, indent=2)

    print(f"OK -> {OUT_FILE} ({len(all_records)} enregistrements)")


if __name__ == "__main__":
    main()
