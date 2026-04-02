"""Configuration centralisée des datasets à ingérer.

Chaque dataset est décrit par :
- name : identifiant unique
- url : URL de téléchargement direct
- filename : nom du fichier local dans data/raw/
- description : rôle dans le pipeline
- extract : si le fichier est une archive, le(s) fichier(s) à extraire
"""

from dataclasses import dataclass, field
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parents[3] / "data"
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"

# Départements Île-de-France
IDF_DEPARTEMENTS = ["75", "77", "78", "91", "92", "93", "94", "95"]

# Codes région Île-de-France
IDF_REGION = "11"


@dataclass
class DatasetConfig:
    name: str
    url: str
    filename: str
    description: str
    extract: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# DVF+ (Cerema) — transactions immobilières géolocalisées
# On utilise les fichiers départementaux IDF pour limiter le volume.
# URL pattern: https://data.cquest.org/dvf_opendata/contrib/cerema-dvf+/
# ---------------------------------------------------------------------------
DVF_PLUS_DATASETS = [
    DatasetConfig(
        name=f"dvf_plus_{dep}",
        url=f"https://data.cquest.org/dvf_opendata/contrib/cerema-dvf+/{dep}.csv.gz",
        filename=f"dvf_plus_{dep}.csv.gz",
        description=f"DVF+ transactions immobilières département {dep}",
    )
    for dep in IDF_DEPARTEMENTS
]

# ---------------------------------------------------------------------------
# Statistiques DVF — agrégats prêts à l'emploi (data.gouv.fr)
# ---------------------------------------------------------------------------
STATS_DVF = DatasetConfig(
    name="stats_dvf",
    url="https://www.data.gouv.fr/fr/datasets/r/c3bef261-5d59-4248-990f-e7a0e40c7738",
    filename="stats_dvf.csv",
    description="Statistiques DVF agrégées : prix médians, volumes, par commune et année",
)

# ---------------------------------------------------------------------------
# Filosofi — revenus communaux
# Fichier "base commune" du dispositif Filosofi
# ---------------------------------------------------------------------------
FILOSOFI_COMMUNES = DatasetConfig(
    name="filosofi_communes",
    url="https://www.insee.fr/fr/statistiques/fichier/8229323/indic-struct-distrib-revenu-2021-COMMUNES.zip",
    filename="filosofi_communes_2021.zip",
    description="Filosofi 2021 — indicateurs de revenus par commune",
    extract=["FILO2021_DEC_COM.csv"],
)

# ---------------------------------------------------------------------------
# Filosofi — revenus IRIS (pour V2)
# ---------------------------------------------------------------------------
FILOSOFI_IRIS = DatasetConfig(
    name="filosofi_iris",
    url="https://www.insee.fr/fr/statistiques/fichier/8229323/indic-struct-distrib-revenu-2021-IRIS.zip",
    filename="filosofi_iris_2021.zip",
    description="Filosofi 2021 — indicateurs de revenus par IRIS (V2)",
    extract=["FILO2021_DEC_IRIS.csv"],
)

# ---------------------------------------------------------------------------
# Population communale — recensement 2021
# ---------------------------------------------------------------------------
POPULATION_COMMUNES = DatasetConfig(
    name="population_communes",
    url="https://www.insee.fr/fr/statistiques/fichier/8202264/base-pop-historiques-1876-2021.zip",
    filename="population_communes_2021.zip",
    description="Population communale historique (recensement 2021)",
    extract=["base-pop-historiques-1876-2021.xlsx"],
)

# ---------------------------------------------------------------------------
# Population par sexe et âge quinquennal — communes
# Fichier détail RP 2021 résultats par commune
# ---------------------------------------------------------------------------
POPULATION_AGE = DatasetConfig(
    name="population_age",
    url="https://www.insee.fr/fr/statistiques/fichier/1893204/BTT_TD_POP1B_2021.zip",
    filename="population_age_2021.zip",
    description="Population par sexe et âge quinquennal, par commune (RP 2021)",
    extract=["BTT_TD_POP1B_2021.csv"],
)

# ---------------------------------------------------------------------------
# Référentiel géographique — code commune / département / région
# Table de passage INSEE COG 2024
# ---------------------------------------------------------------------------
COG_COMMUNES = DatasetConfig(
    name="cog_communes",
    url="https://www.insee.fr/fr/statistiques/fichier/7766585/v_commune_2024.csv",
    filename="cog_communes_2024.csv",
    description="Code Officiel Géographique 2024 — communes",
)


# ---------------------------------------------------------------------------
# Tous les datasets V1 (commune × année, Île-de-France)
# ---------------------------------------------------------------------------
ALL_V1_DATASETS: list[DatasetConfig] = [
    STATS_DVF,
    FILOSOFI_COMMUNES,
    POPULATION_COMMUNES,
    POPULATION_AGE,
    COG_COMMUNES,
] + DVF_PLUS_DATASETS

# Datasets V2 (IRIS)
ALL_V2_DATASETS: list[DatasetConfig] = [
    FILOSOFI_IRIS,
]
