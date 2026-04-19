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
# URL pattern: https://files.data.gouv.fr/geo-dvf/latest/csv/<année>/departements/
# On télécharge chaque année séparément pour avoir l'historique complet.
# ---------------------------------------------------------------------------
# files.data.gouv.fr `latest/` ne garde plus 2020 (rotation). Si besoin
# d'historique plus long, basculer sur une URL versionnée.
DVF_ANNEES = ["2021", "2022", "2023", "2024", "2025"]

DVF_PLUS_DATASETS = [
    DatasetConfig(
        name=f"dvf_plus_{dep}_{annee}",
        url=f"https://files.data.gouv.fr/geo-dvf/latest/csv/{annee}/departements/{dep}.csv.gz",
        filename=f"dvf_plus_{dep}_{annee}.csv.gz",
        description=f"DVF géolocalisé département {dep} année {annee}",
    )
    for annee in DVF_ANNEES
    for dep in IDF_DEPARTEMENTS
]

# ---------------------------------------------------------------------------
# Statistiques DVF — agrégats prêts à l'emploi (data.gouv.fr)
# ---------------------------------------------------------------------------
STATS_DVF = DatasetConfig(
    name="stats_dvf",
    url="https://www.data.gouv.fr/api/1/datasets/r/851d342f-9c96-41c1-924a-11a7a7aae8a6",
    filename="stats_dvf.csv",
    description="Statistiques DVF agrégées : prix médians, volumes, par commune et année",
)

# ---------------------------------------------------------------------------
# Filosofi — revenus communaux
# Fichier "base commune" du dispositif Filosofi
# ---------------------------------------------------------------------------
FILOSOFI_COMMUNES = DatasetConfig(
    name="filosofi_communes",
    url="https://www.insee.fr/fr/statistiques/fichier/7756855/indic-struct-distrib-revenu-2021-COMMUNES_csv.zip",
    filename="filosofi_communes_2021.zip",
    description="Filosofi 2021 — indicateurs de revenus par commune",
    extract=["FILO2021_DEC_COM.csv"],
)

# ---------------------------------------------------------------------------
# Filosofi — revenus IRIS (pour V2)
# ---------------------------------------------------------------------------
FILOSOFI_IRIS = DatasetConfig(
    name="filosofi_iris",
    url="https://www.insee.fr/fr/statistiques/fichier/7756855/indic-struct-distrib-revenu-2021-IRIS_csv.zip",
    filename="filosofi_iris_2021.zip",
    description="Filosofi 2021 — indicateurs de revenus par IRIS (V2)",
    extract=["FILO2021_DEC_IRIS.csv"],
)

# ---------------------------------------------------------------------------
# Population communale — recensement 2021
# ---------------------------------------------------------------------------
POPULATION_COMMUNES = DatasetConfig(
    name="population_communes",
    url="https://www.insee.fr/fr/statistiques/fichier/3698339/base-pop-historiques-1876-2023.xlsx",
    filename="base-pop-historiques-1876-2023.xlsx",
    description="Population communale historique (recensement 2023)",
)

# ---------------------------------------------------------------------------
# Population par sexe et âge quinquennal — communes
# Fichier détail RP 2021 résultats par commune
# ---------------------------------------------------------------------------
POPULATION_AGE = DatasetConfig(
    name="population_age",
    url="https://www.insee.fr/fr/statistiques/fichier/8202264/TD_POP1B_2021_csv.zip",
    filename="population_age_2021.zip",
    description="Population par sexe et âge quinquennal, par commune (RP 2021)",
    extract=["TD_POP1B_2021.csv"],
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
# Carte des loyers — loyers médians par commune (data.gouv.fr / ANIL)
# ---------------------------------------------------------------------------
LOYERS_COMMUNES = DatasetConfig(
    name="loyers_communes",
    url="https://www.data.gouv.fr/api/1/datasets/r/55b34088-0964-415f-9df7-d87dd98a09be",
    filename="loyers_communes_2025.csv",
    description="Carte des loyers 2025 — loyer prédit au m² par commune (appartements)",
)

# ---------------------------------------------------------------------------
# Délinquance — crimes et délits par commune (Ministère Intérieur / SSMSI)
# ---------------------------------------------------------------------------
DELINQUANCE_COMMUNES = DatasetConfig(
    name="delinquance_communes",
    url="https://www.data.gouv.fr/api/1/datasets/r/44ef4323-1097-48d5-8719-3c544b55d294",
    filename="delinquance_communes.csv.gz",
    description="Délinquance enregistrée par commune depuis 2016 (police + gendarmerie)",
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
    LOYERS_COMMUNES,
    DELINQUANCE_COMMUNES,
] + DVF_PLUS_DATASETS

# Datasets V2 (IRIS)
ALL_V2_DATASETS: list[DatasetConfig] = [
    FILOSOFI_IRIS,
]
