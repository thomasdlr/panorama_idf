"""Setup Metabase with PostgreSQL and pre-configured dashboard.

Usage:
    uv run python scripts/setup_metabase.py

Exports dbt mart tables from DuckDB to PostgreSQL, then configures
Metabase with questions and a dashboard.
"""

import subprocess
import sys
import time
from pathlib import Path

import duckdb
import httpx

METABASE_URL = "http://localhost:3000"
ADMIN_EMAIL = "admin@france-aujourdhui.local"
ADMIN_PASSWORD = "FranceAujourdhui2024!"

PG_CONN = "host=localhost port=5432 dbname=france_aujourdhui user=metabase password=metabase"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAIN_DB_PATH = PROJECT_ROOT / "data" / "france_aujourdhui.duckdb"

MART_TABLES = [
    "main_marts.mart_immo__accessibilite_commune",
    "main_marts.mart_immo__ranking_tension",
    "main_marts.mart_immo__synthese_zone",
    "main_marts.mart_immo__evolution_prix",
]


def start_services() -> None:
    """Start PostgreSQL and Metabase via docker compose."""
    subprocess.run(
        ["docker", "compose", "up", "-d"],
        cwd=PROJECT_ROOT,
        check=True,
    )
    # Wait for PostgreSQL to be ready
    print("  Attente de PostgreSQL…", end="", flush=True)
    for _ in range(30):
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "postgres",
             "pg_isready", "-U", "metabase", "-d", "france_aujourdhui"],
            cwd=PROJECT_ROOT,
            capture_output=True,
        )
        if result.returncode == 0:
            print(" OK")
            return
        print(".", end="", flush=True)
        time.sleep(2)
    print(" TIMEOUT")
    sys.exit(1)


def export_marts_to_postgres() -> None:
    """Export mart tables from DuckDB to PostgreSQL."""
    con = duckdb.connect(str(MAIN_DB_PATH), read_only=True)
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")

    for table in MART_TABLES:
        short_name = table.split(".")[-1]
        con.execute(f"DROP TABLE IF EXISTS pg.public.{short_name}")
        con.execute(
            f"CREATE TABLE pg.public.{short_name} AS SELECT * FROM {table}"
        )
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {short_name} — {rows:,} lignes")

    con.execute("DETACH pg")
    con.close()


def wait_for_metabase(client: httpx.Client, timeout: int = 180) -> None:
    """Wait for Metabase to be ready."""
    print("  Attente du démarrage de Metabase…", end="", flush=True)
    start = time.time()
    while time.time() - start < timeout:
        try:
            r = client.get("/api/health")
            if r.status_code == 200 and r.json().get("status") == "ok":
                print(" OK")
                return
        except httpx.ConnectError:
            pass
        print(".", end="", flush=True)
        time.sleep(3)
    print(" TIMEOUT")
    sys.exit(1)


def setup_admin(client: httpx.Client) -> str:
    """Complete initial Metabase setup, return session token."""
    r = client.get("/api/session/properties")
    props = r.json()
    if props.get("setup-token") is None:
        print("  Metabase déjà configuré, connexion…")
        r = client.post("/api/session", json={
            "username": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
        })
        r.raise_for_status()
        return r.json()["id"]

    setup_token = props["setup-token"]
    print("  Configuration initiale de Metabase…")
    r = client.post("/api/setup", json={
        "token": setup_token,
        "user": {
            "email": ADMIN_EMAIL,
            "password": ADMIN_PASSWORD,
            "first_name": "Admin",
            "last_name": "France Aujourd'hui",
            "site_name": "France Aujourd'hui",
        },
        "prefs": {
            "site_name": "France Aujourd'hui",
            "site_locale": "fr",
            "allow_tracking": False,
        },
    })
    r.raise_for_status()
    return r.json()["id"]


def add_postgres_database(client: httpx.Client) -> int:
    """Add PostgreSQL database connection to Metabase, return database ID."""
    r = client.get("/api/database")
    for db in r.json().get("data", []):
        if db.get("name") == "France Aujourd'hui":
            print(f"  Base de données déjà configurée (id={db['id']})")
            client.post(f"/api/database/{db['id']}/sync_schema")
            return db["id"]

    print("  Ajout de la base PostgreSQL…")
    r = client.post("/api/database", json={
        "name": "France Aujourd'hui",
        "engine": "postgres",
        "details": {
            "host": "postgres",
            "port": 5432,
            "dbname": "france_aujourdhui",
            "user": "metabase",
            "password": "metabase",
        },
    })
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"  Base ajoutée (id={db_id})")

    # Wait for sync
    print("  Synchronisation du schéma…", end="", flush=True)
    for _ in range(30):
        time.sleep(2)
        r = client.get(f"/api/database/{db_id}")
        status = r.json().get("initial_sync_status")
        if status == "complete":
            print(" OK")
            return db_id
        print(".", end="", flush=True)
    print(" (sync en cours, on continue)")
    return db_id


def create_question(
    client: httpx.Client,
    name: str,
    db_id: int,
    query: str,
    display: str = "table",
    description: str = "",
    visualization_settings: dict | None = None,
) -> int:
    """Create a saved question (card), return card ID."""
    r = client.get("/api/card")
    for card in r.json():
        if card.get("name") == name:
            print(f"  Question déjà existante : {name} (id={card['id']})")
            return card["id"]

    r = client.post("/api/card", json={
        "name": name,
        "description": description or None,
        "dataset_query": {
            "type": "native",
            "native": {"query": query},
            "database": db_id,
        },
        "display": display,
        "visualization_settings": visualization_settings or {},
    })
    r.raise_for_status()
    card_id = r.json()["id"]
    print(f"  Question créée : {name} (id={card_id})")
    return card_id


def create_dashboard_with_cards(
    client: httpx.Client, cards: list[tuple[int, dict]],
) -> int:
    """Create a dashboard and add cards using layout from card definitions."""
    dash_name = "Accessibilite immobiliere en Ile-de-France"

    r = client.get("/api/dashboard")
    for d in r.json():
        if d.get("name") == dash_name:
            print(f"  Dashboard déjà existant (id={d['id']})")
            return d["id"]

    r = client.post("/api/dashboard", json={
        "name": dash_name,
        "description": (
            "Indicateurs croisant prix immobiliers, revenus locaux et "
            "structure demographique pour les communes d'Ile-de-France (2020-2024). "
            "Sources : DVF (data.gouv.fr), Filosofi 2021 (INSEE), RP 2021 (INSEE)."
        ),
    })
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"  Dashboard créé (id={dash_id})")

    card_defs = []
    for i, (card_id, c) in enumerate(cards):
        card_defs.append({
            "id": -i - 1,
            "card_id": card_id,
            "row": c["row"],
            "col": c["col"],
            "size_x": c["w"],
            "size_y": c["h"],
        })

    r = client.put(f"/api/dashboard/{dash_id}", json={
        "dashcards": card_defs,
    })
    r.raise_for_status()
    print(f"  {len(cards)} cartes ajoutées au dashboard")
    return dash_id


# ── Cards definition (PostgreSQL queries + layout) ────────────────
# annee is cast to text to prevent Metabase treating it as continuous number.
# Layout: row/col/w/h define position on 18-column grid.

CARDS = [
    {
        "name": "Synthese Paris / Petite couronne / Grande couronne",
        "display": "table",
        "description": (
            "Agregats ponderes par zone IDF et annee. "
            "Paris = arrondissements (75), Petite couronne = 92/93/94, "
            "Grande couronne = 77/78/91/95."
        ),
        "query": """
SELECT zone_idf, annee::text as annee, nb_communes, nb_ventes_total,
       round(prix_m2_median_pondere::numeric) as prix_m2_median,
       round(niveau_vie_median_pondere::numeric) as niveau_vie_median,
       round(ratio_achat_revenu_pondere::numeric, 1) as ratio_achat_revenu,
       round((part_25_39_ponderee * 100)::numeric, 1) as pct_25_39
FROM mart_immo__synthese_zone
ORDER BY annee DESC, zone_idf
""",
        "row": 0, "col": 0, "w": 18, "h": 7,
    },
    {
        "name": "Prix median au m2 par zone IDF",
        "display": "line",
        "description": "Prix median au m2 pondere par volume de ventes (2020-2024).",
        "query": """
SELECT annee::text as annee, zone_idf,
       round(prix_m2_median_pondere::numeric) as prix_m2_median
FROM mart_immo__synthese_zone
ORDER BY annee, zone_idf
""",
        "visualization_settings": {
            "graph.dimensions": ["annee", "zone_idf"],
            "graph.metrics": ["prix_m2_median"],
            "graph.y_axis.title_text": "Prix median au m2 (EUR)",
        },
        "row": 7, "col": 0, "w": 9, "h": 7,
    },
    {
        "name": "Prix au m2 par arrondissement parisien",
        "display": "line",
        "description": (
            "Prix median au m2 par arrondissement (75101-75120). "
            "DVF utilise les codes arrondissement, pas 75056."
        ),
        "query": """
SELECT annee::text as annee, nom_commune,
       round(prix_m2_median::numeric) as prix_m2_median
FROM mart_immo__evolution_prix
WHERE code_commune LIKE '751%'
ORDER BY annee, nom_commune
""",
        "visualization_settings": {
            "graph.dimensions": ["annee", "nom_commune"],
            "graph.metrics": ["prix_m2_median"],
            "graph.y_axis.title_text": "Prix median au m2 (EUR)",
        },
        "row": 7, "col": 9, "w": 9, "h": 7,
    },
    {
        "name": "Volume de ventes par zone et annee",
        "display": "bar",
        "description": "Transactions (appartements + maisons) par zone et annee.",
        "query": """
SELECT annee::text as annee, zone_idf, nb_ventes_total
FROM mart_immo__synthese_zone
ORDER BY annee, zone_idf
""",
        "visualization_settings": {
            "graph.dimensions": ["annee", "zone_idf"],
            "graph.metrics": ["nb_ventes_total"],
            "graph.y_axis.title_text": "Nombre de ventes",
            "stackable.stack_type": "stacked",
        },
        "row": 14, "col": 0, "w": 9, "h": 7,
    },
    {
        "name": "Distribution indice de tension (2024)",
        "display": "bar",
        "description": (
            "Repartition des communes IDF par tranche de tension. "
            "Score 0-100 : ratio prix/revenu (40%), prix m2/revenu (30%), "
            "faible presence 25-39 ans (15%), proxy pauvrete (15%)."
        ),
        "query": """
SELECT
    CASE
        WHEN indice_tension < 20 THEN '1. Faible (0-20)'
        WHEN indice_tension < 40 THEN '2. Modere (20-40)'
        WHEN indice_tension < 60 THEN '3. Moyen (40-60)'
        WHEN indice_tension < 80 THEN '4. Eleve (60-80)'
        ELSE '5. Tres eleve (80-100)'
    END as tension,
    count(*) as nb_communes
FROM mart_immo__ranking_tension
WHERE annee = 2024
GROUP BY 1
ORDER BY 1
""",
        "visualization_settings": {
            "graph.dimensions": ["tension"],
            "graph.metrics": ["nb_communes"],
        },
        "row": 14, "col": 9, "w": 9, "h": 7,
    },
    {
        "name": "Jeunes adultes vs prix (2024)",
        "display": "scatter",
        "description": (
            "Chaque point = une commune IDF. X = prix m2, Y = part 25-39 ans. "
            "Taille = indice de tension. Filtrable par zone."
        ),
        "query": """
SELECT nom_commune, zone_idf, prix_m2_median,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(indice_tension::numeric, 1) as indice_tension
FROM mart_immo__ranking_tension
WHERE annee = 2024
  AND part_25_39 IS NOT NULL
  AND ({{zone_filter}} = 'Toutes' OR zone_idf = {{zone_filter}})
ORDER BY prix_m2_median DESC
""",
        "template_tags": {
            "zone_filter": {
                "name": "zone_filter",
                "display-name": "Zone IDF",
                "type": "text",
                "default": "Toutes",
            },
        },
        "visualization_settings": {
            "graph.x_axis.title_text": "Prix median au m2 (EUR)",
            "graph.y_axis.title_text": "Part des 25-39 ans (%)",
            "graph.dimensions": ["prix_m2_median"],
            "graph.metrics": ["pct_25_39"],
            "scatter.bubble": "indice_tension",
        },
        "row": 21, "col": 0, "w": 12, "h": 9,
    },
    {
        "name": "Top 20 communes les plus tendues (2024)",
        "display": "table",
        "description": (
            "Classement par indice de tension (0-100). "
            "Ratio prix/revenu (40%), prix m2/revenu (30%), "
            "faible 25-39 ans (15%), proxy pauvrete (15%). "
            "Percentile rank par composante."
        ),
        "query": """
SELECT rang_tension as rang, nom_commune, code_departement as dept, zone_idf,
       round(prix_m2_median::numeric) as prix_m2,
       round(niveau_vie_median::numeric) as revenu_median,
       round(ratio_achat_revenu_annuel::numeric, 1) as ratio_achat,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(indice_tension::numeric, 1) as tension
FROM mart_immo__ranking_tension
WHERE annee = 2024
ORDER BY rang_tension
LIMIT 20
""",
        "row": 21, "col": 12, "w": 6, "h": 9,
    },
    {
        "name": "Communes les plus accessibles - Petite couronne (2024)",
        "display": "table",
        "description": (
            "Petite couronne (92/93/94) par ratio d'achat croissant. "
            "Ratio = prix median / niveau de vie annuel (Filosofi 2021)."
        ),
        "query": """
SELECT nom_commune, code_departement as dept,
       round(prix_m2_median::numeric) as prix_m2,
       round(niveau_vie_median::numeric) as revenu_median,
       round(ratio_achat_revenu_annuel::numeric, 1) as ratio_achat,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39
FROM mart_immo__accessibilite_commune
WHERE zone_idf = 'Petite couronne' AND annee = 2024
ORDER BY ratio_achat_revenu_annuel ASC
LIMIT 15
""",
        "row": 30, "col": 0, "w": 9, "h": 8,
    },
]


def main() -> None:
    print("\n═══ Setup Metabase — France Aujourd'hui ═══\n")

    # 1. Start services
    print("[1/6] Démarrage PostgreSQL + Metabase")
    start_services()

    # 2. Export marts to PostgreSQL
    print("\n[2/6] Export des marts vers PostgreSQL")
    export_marts_to_postgres()

    client = httpx.Client(base_url=METABASE_URL, timeout=30)

    # 3. Wait for Metabase
    print("\n[3/6] Connexion à Metabase")
    wait_for_metabase(client)

    # 4. Initial setup
    print("\n[4/6] Configuration admin")
    session_id = setup_admin(client)
    client.headers["X-Metabase-Session"] = session_id

    # 5. Add PostgreSQL database
    print("\n[5/6] Base de données PostgreSQL")
    db_id = add_postgres_database(client)

    # 6. Create questions and dashboard
    print("\n[6/6] Questions et dashboard")
    card_ids = []
    for c in CARDS:
        payload = {
            "name": c["name"],
            "description": c.get("description"),
            "dataset_query": {
                "type": "native",
                "native": {"query": c["query"]},
                "database": db_id,
            },
            "display": c["display"],
            "visualization_settings": c.get("visualization_settings", {}),
        }
        if "template_tags" in c:
            payload["dataset_query"]["native"]["template-tags"] = c["template_tags"]
        card_id = create_question(
            client, c["name"], db_id, c["query"], c["display"],
            description=c.get("description", ""),
            visualization_settings=c.get("visualization_settings"),
        )
        card_ids.append((card_id, c))

    dash_id = create_dashboard_with_cards(client, card_ids)

    print(f"\n{'═' * 50}")
    print(f"  Metabase prêt : {METABASE_URL}")
    print(f"  Dashboard : {METABASE_URL}/dashboard/{dash_id}")
    print(f"  Login : {ADMIN_EMAIL} / {ADMIN_PASSWORD}")
    print(f"{'═' * 50}\n")

    client.close()


if __name__ == "__main__":
    main()
