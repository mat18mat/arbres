import json
from pathlib import Path
import pandas as pd
import re


RAW_DIR = Path("data-raw")
OUT_DIR = Path("data")
OUT_FILE = OUT_DIR / "arbres.json"

# Tes fichiers (on s’adapte à ce que tu as)
PARIS_FILE = RAW_DIR / "les-arbres.csv"
HDS_FILE = RAW_DIR / "arbres-remarquables-du-territoire-des-hauts-de-seine-hors-proprietes-privees.csv"


def parse_geo_point_2d(val):
    """
    Dans tes datasets, geo_point_2d ressemble souvent à: "48.8326, 2.41145"
    => (lat, lon). On renvoie {"lon": ..., "lat": ...} comme sur la photo.
    """
    if pd.isna(val):
        return {"lon": None, "lat": None}
    s = str(val).strip()
    if not s:
        return {"lon": None, "lat": None}

    # parfois séparateur virgule
    parts = [p.strip() for p in s.split(",")]
    if len(parts) < 2:
        return {"lon": None, "lat": None}

    try:
        a = float(parts[0].replace(",", "."))
        b = float(parts[1].replace(",", "."))
    except ValueError:
        return {"lon": None, "lat": None}

    # Heuristique: si a ressemble à une latitude (≈48) et b à une longitude (≈2)
    # alors a=lat, b=lon. Sinon on inverse.
    if -90 <= a <= 90 and -180 <= b <= 180:
        lat, lon = a, b
    else:
        lon, lat = a, b

    return {"lon": lon, "lat": lat}


def to_float(val):
    if pd.isna(val):
        return None
    try:
        return float(str(val).replace(",", ".").strip())
    except ValueError:
        return None


def circonference_to_m(val):
    """
    Photo: circonference = 3.05 (m) alors que Paris donne CIRCONFERENCE (cm).
    Donc:
    - Si valeur > 20, on suppose que c'est en cm -> /100
    - Sinon on la garde (déjà en m)
    """
    x = to_float(val)
    if x is None:
        return None
    if x > 20:  # probablement cm
        return round(x / 100.0, 2)
    return round(x, 2)


def compute_insee_from_arrondissement(arr):
    """
    Gère :
    - "14" → 75114
    - "PARIS 14E ARRDT" → 75114
    - "PARIS 7E ARRDT" → 75107
    Sinon None
    """
    if pd.isna(arr):
        return None

    s = str(arr).strip().upper()

    # cherche un nombre entre 1 et 20 dans la chaîne
    m = re.search(r"\b([1-9]|1\d|20)\b", s)
    if not m:
        return None

    n = int(m.group(1))
    return f"751{n:02d}"




def unify_paris(df):
    """
    En-tête Paris:
    IDBASE;...;ARRONDISSEMENT;...;LIBELLE FRANCAIS;GENRE;ESPECE;...;CIRCONFERENCE (cm);HAUTEUR (m);...;REMARQUABLE;geo_point_2d
    """
    # Normalise colonnes (on garde les noms originaux en majuscules ici)
    out = []

    for _, r in df.iterrows():
        record = {
            "source": "paris",
            "commune": None,
            "code_insee": None,
            "nom": None,
            "latin": None,
            "hauteur": None,
            "circonference": None,
            "localisation": {"lon": None, "lat": None},
        }

        # commune = ARRONDISSEMENT (dans ton exemple ça peut être "BOIS DE VINCENNES")
        commune = r.get("ARRONDISSEMENT")
        if not pd.isna(commune):
            record["commune"] = str(commune).strip()

        # code_insee calculé si arrondissement numérique (1..20)
        record["code_insee"] = compute_insee_from_arrondissement(r.get("ARRONDISSEMENT"))

        # nom = LIBELLE FRANCAIS
        nom = r.get("LIBELLE FRANCAIS")
        if not pd.isna(nom):
            record["nom"] = str(nom).strip()

        # latin = "GENRE ESPECE" (ex: Taxodium distichum)
        genre = r.get("GENRE")
        espece = r.get("ESPECE")
        g = "" if pd.isna(genre) else str(genre).strip()
        e = "" if pd.isna(espece) else str(espece).strip()
        latin = (g + " " + e).strip()
        record["latin"] = latin if latin else None

        # hauteur = HAUTEUR (m)
        record["hauteur"] = to_float(r.get("HAUTEUR (m)"))

        # circonference en mètres (photo)
        record["circonference"] = circonference_to_m(r.get("CIRCONFERENCE (cm)"))

        # localisation
        record["localisation"] = parse_geo_point_2d(r.get("geo_point_2d"))

        out.append(record)

    return out


def unify_hds(df):
    """
    En-tête HDS:
    COMMUNE;DOMAINE;CODE_INSEE;...;NOM_FRANCAIS;NOM_LATIN;...;HAUTEUR;CIRCONFERENCE;...;geo_point_2d
    """
    out = []

    for _, r in df.iterrows():
        record = {
            "source": "hauts-de-seine",
            "commune": None,
            "code_insee": None,
            "nom": None,
            "latin": None,
            "hauteur": None,
            "circonference": None,
            "localisation": {"lon": None, "lat": None},
        }

        # commune
        commune = r.get("COMMUNE")
        if not pd.isna(commune):
            record["commune"] = str(commune).strip()

        # code_insee (déjà fourni)
        ci = r.get("CODE_INSEE")
        if not pd.isna(ci):
            record["code_insee"] = str(ci).strip()

        # nom / latin
        nf = r.get("NOM_FRANCAIS")
        if not pd.isna(nf):
            record["nom"] = str(nf).strip()

        nl = r.get("NOM_LATIN")
        if not pd.isna(nl):
            record["latin"] = str(nl).strip()

        # hauteur / circonference (souvent en m, mais on sécurise)
        record["hauteur"] = to_float(r.get("HAUTEUR"))
        record["circonference"] = circonference_to_m(r.get("CIRCONFERENCE"))

        # localisation
        record["localisation"] = parse_geo_point_2d(r.get("geo_point_2d"))

        out.append(record)

    return out


def main():
    if not PARIS_FILE.exists():
        raise FileNotFoundError(f"Fichier manquant: {PARIS_FILE}")
    if not HDS_FILE.exists():
        raise FileNotFoundError(f"Fichier manquant: {HDS_FILE}")

    # CSV avec ';' (vu dans tes en-têtes)
    df_paris = pd.read_csv(PARIS_FILE, sep=";", engine="python", encoding="latin-1")
    df_hds = pd.read_csv(HDS_FILE, sep=";", engine="python")

    # (Option) Si tu veux vraiment garder uniquement les remarquables côté Paris:
    # REMARQUABLE = "OUI" (dans ton header c’est REMARQUABLE)
    if "REMARQUABLE" in df_paris.columns:
        df_paris = df_paris[df_paris["REMARQUABLE"].astype(str).str.upper().isin(["OUI", "TRUE", "1"])]

    OUT_DIR.mkdir(exist_ok=True)

    records = []
    records.extend(unify_paris(df_paris))
    records.extend(unify_hds(df_hds))

    with open(OUT_FILE, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    print(f"OK -> {OUT_FILE} ({len(records)} enregistrements)")


if __name__ == "__main__":
    main()
