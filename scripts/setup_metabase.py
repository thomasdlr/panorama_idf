"""Setup Metabase with PostgreSQL and pre-configured tabbed dashboard.

Usage:
    uv run python scripts/setup_metabase.py

Exports dbt mart tables from DuckDB to PostgreSQL, generates GeoJSON maps,
then configures Metabase with 3-tab dashboard (IDF, Paris, Petite couronne).
"""

import json
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import duckdb
import httpx

METABASE_URL = "http://localhost:3000"
ADMIN_EMAIL = "admin@france-aujourdhui.local"
ADMIN_PASSWORD = "FranceAujourdhui2024!"

PG_CONN = "host=localhost port=5480 dbname=france_aujourdhui user=metabase password=metabase"

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAIN_DB_PATH = PROJECT_ROOT / "data" / "france_aujourdhui.duckdb"
GEOJSON_DIR = PROJECT_ROOT / "data" / "metabase"

MART_TABLES = [
    "main_marts.mart_immo__accessibilite_commune",
    "main_marts.mart_immo__ranking_tension",
    "main_marts.mart_immo__synthese_zone",
    "main_marts.mart_immo__evolution_prix",
]

LATEST_YEAR = 2025


# ── Infrastructure ──────────────────────────────────────────────────


def start_services() -> None:
    """Start PostgreSQL and Metabase via docker compose."""
    subprocess.run(["docker", "compose", "up", "-d"], cwd=PROJECT_ROOT, check=True)
    print("  Attente de PostgreSQL…", end="", flush=True)
    for _ in range(30):
        result = subprocess.run(
            ["docker", "compose", "exec", "-T", "postgres",
             "pg_isready", "-U", "metabase", "-d", "france_aujourdhui"],
            cwd=PROJECT_ROOT, capture_output=True,
        )
        if result.returncode == 0:
            print(" OK")
            subprocess.run(
                ["docker", "compose", "exec", "-T", "postgres",
                 "psql", "-U", "metabase", "-d", "france_aujourdhui",
                 "-c", "CREATE DATABASE metabase_app OWNER metabase;"],
                cwd=PROJECT_ROOT, capture_output=True,
            )
            return
        print(".", end="", flush=True)
        time.sleep(2)
    print(" TIMEOUT")
    sys.exit(1)


def export_marts_to_postgres() -> None:
    """Export mart tables from DuckDB to PostgreSQL."""
    tmp = Path(tempfile.mktemp(suffix=".duckdb"))
    shutil.copy2(MAIN_DB_PATH, tmp)
    con = duckdb.connect(str(tmp))
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")

    for table in MART_TABLES:
        short_name = table.split(".")[-1]
        con.execute(f"DROP TABLE IF EXISTS pg.public.{short_name}")
        con.execute(f"CREATE TABLE pg.public.{short_name} AS SELECT * FROM {table}")
        rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
        print(f"  {short_name} — {rows:,} lignes")

    con.execute("DETACH pg")
    con.close()
    tmp.unlink(missing_ok=True)


def generate_geojson() -> None:
    """Download and generate zone-specific GeoJSON files for maps."""
    GEOJSON_DIR.mkdir(parents=True, exist_ok=True)

    if (GEOJSON_DIR / "idf_communes.geojson").exists():
        print("  GeoJSON déjà présent")
        return

    def simplify_coords(coords, precision=4):
        if isinstance(coords[0], (int, float)):
            return [round(c, precision) for c in coords]
        return [simplify_coords(c, precision) for c in coords]

    # Download IDF communes + Paris arrondissements
    communes = httpx.get(
        "https://geo.api.gouv.fr/communes?codeRegion=11&format=geojson&geometry=contour",
        timeout=30,
    ).json()
    arrondissements = httpx.get(
        "https://geo.api.gouv.fr/communes?codeDepartement=75"
        "&format=geojson&geometry=contour&type=arrondissement-municipal",
        timeout=30,
    ).json()

    # Merge: replace Paris 75056 with 20 arrondissements
    features = [f for f in communes["features"] if f["properties"]["code"] != "75056"]
    features.extend(arrondissements["features"])

    # Simplify and strip properties
    for feat in features:
        feat["geometry"]["coordinates"] = simplify_coords(feat["geometry"]["coordinates"])
        props = feat["properties"]
        feat["properties"] = {"code": props["code"], "nom": props["nom"]}

    idf = {"type": "FeatureCollection", "features": features}

    # Write zone-specific files
    def write_geo(name, feats):
        path = GEOJSON_DIR / f"{name}.geojson"
        with open(path, "w") as f:
            json.dump({"type": "FeatureCollection", "features": feats}, f, separators=(",", ":"))
        print(f"  {name}.geojson — {len(feats)} features ({path.stat().st_size // 1024}KB)")

    write_geo("idf_communes", features)
    write_geo("paris_arrondissements",
              [f for f in features if f["properties"]["code"].startswith("751")])
    write_geo("petite_couronne",
              [f for f in features if f["properties"]["code"][:2] in ("92", "93", "94")])
    write_geo("grande_couronne",
              [f for f in features if f["properties"]["code"][:2] in ("77", "78", "91", "95")])


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
        except (httpx.ConnectError, httpx.RemoteProtocolError, httpx.ReadError):
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
            "username": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
        })
        r.raise_for_status()
        return r.json()["id"]

    print("  Configuration initiale de Metabase…")
    r = client.post("/api/setup", json={
        "token": props["setup-token"],
        "user": {
            "email": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
            "first_name": "Admin", "last_name": "France Aujourd'hui",
            "site_name": "France Aujourd'hui",
        },
        "prefs": {"site_name": "France Aujourd'hui", "site_locale": "fr", "allow_tracking": False},
    })
    r.raise_for_status()
    return r.json()["id"]


def add_postgres_database(client: httpx.Client) -> int:
    """Add PostgreSQL database connection to Metabase."""
    r = client.get("/api/database")
    for db in r.json().get("data", []):
        if db.get("name") == "France Aujourd'hui":
            print(f"  Base déjà configurée (id={db['id']})")
            client.post(f"/api/database/{db['id']}/sync_schema")
            return db["id"]

    print("  Ajout de la base PostgreSQL…")
    r = client.post("/api/database", json={
        "name": "France Aujourd'hui", "engine": "postgres",
        "details": {"host": "postgres", "port": 5432, "dbname": "france_aujourdhui",
                     "user": "metabase", "password": "metabase"},
    })
    r.raise_for_status()
    db_id = r.json()["id"]
    print(f"  Base ajoutée (id={db_id})")

    print("  Synchronisation…", end="", flush=True)
    for _ in range(30):
        time.sleep(2)
        r = client.get(f"/api/database/{db_id}")
        if r.json().get("initial_sync_status") == "complete":
            print(" OK")
            return db_id
        print(".", end="", flush=True)
    print(" (on continue)")
    return db_id


def register_geojson_maps(client: httpx.Client) -> None:
    """Register custom GeoJSON maps in Metabase settings."""
    r = client.get("/api/setting/custom-geojson")
    maps = r.json()
    for key, name in [
        ("idf_communes", "Communes Ile-de-France"),
        ("paris_arr", "Arrondissements Paris"),
        ("petite_couronne", "Communes Petite couronne"),
        ("grande_couronne", "Communes Grande couronne"),
    ]:
        filename = f"{key.replace('paris_arr', 'paris_arrondissements')}.geojson"
        maps[key] = {
            "name": name,
            "url": f"http://geojson:80/{filename}",
            "region_key": "code",
            "region_name": "nom",
        }
    client.put("/api/setting/custom-geojson", json={"value": maps})
    print("  4 cartes GeoJSON enregistrées")


# ── Card helper ─────────────────────────────────────────────────────


def make_card(client: httpx.Client, db_id: int, name: str, display: str,
              query: str, desc: str = "", viz: dict | None = None) -> int:
    """Create a Metabase card (question)."""
    r = client.post("/api/card", json={
        "name": name, "description": desc or None,
        "dataset_query": {"type": "native", "native": {"query": query}, "database": db_id},
        "display": display, "visualization_settings": viz or {},
    })
    r.raise_for_status()
    cid = r.json()["id"]
    print(f"  {cid}: {name}")
    return cid


def map_viz(region: str, metric: str) -> dict:
    return {"map.type": "region", "map.region": region,
            "map.dimension": "code_commune", "map.metric": metric}


# ── Dashboard creation ──────────────────────────────────────────────

Y = str(LATEST_YEAR)


def create_tabbed_dashboard(client: httpx.Client, db_id: int) -> int:
    """Create the full 3-tab dashboard with all cards."""

    # ── IDF tab cards ──
    c_idf_map_prix = make_card(client, db_id,
        "Prix au m2 — Ile-de-France", "map",
        f"SELECT code_commune, nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes, zone_idf\nFROM mart_immo__accessibilite_commune WHERE annee = {Y}",
        "Prix median au m2 par commune.", map_viz("idf_communes", "prix_m2"))

    c_idf_map_age = make_card(client, db_id,
        "Part des 25-39 ans — Ile-de-France", "map",
        f"SELECT code_commune, nom_commune, round((part_25_39 * 100)::numeric, 1) as pct_25_39, zone_idf\nFROM mart_immo__accessibilite_commune WHERE annee = {Y}",
        "Part des 25-39 ans (RP 2021).", map_viz("idf_communes", "pct_25_39"))

    c_idf_synthese = make_card(client, db_id,
        "Synthese par zone", "table",
        """SELECT zone_idf, annee::text as annee, nb_communes, nb_ventes_total,
       round(prix_m2_median_pondere::numeric) as prix_m2,
       round(niveau_vie_median_pondere::numeric) as niveau_vie,
       round(ratio_achat_revenu_pondere::numeric, 1) as ratio_achat,
       round((part_25_39_ponderee * 100)::numeric, 1) as pct_25_39
FROM mart_immo__synthese_zone ORDER BY annee DESC, zone_idf""",
        "Agregats ponderes. Paris = 75, PC = 92/93/94, GC = 77/78/91/95.")

    c_idf_prix = make_card(client, db_id,
        "Evolution prix au m2 par zone", "line",
        """SELECT annee::text as annee, zone_idf, round(prix_m2_median_pondere::numeric) as prix_m2
FROM mart_immo__synthese_zone ORDER BY annee, zone_idf""",
        viz={"graph.dimensions": ["annee", "zone_idf"], "graph.metrics": ["prix_m2"],
             "graph.y_axis.title_text": "EUR / m2"})

    c_idf_volume = make_card(client, db_id,
        "Volume de ventes par zone", "bar",
        """SELECT annee::text as annee, zone_idf, nb_ventes_total
FROM mart_immo__synthese_zone ORDER BY annee, zone_idf""",
        viz={"graph.dimensions": ["annee", "zone_idf"], "graph.metrics": ["nb_ventes_total"],
             "graph.y_axis.title_text": "Ventes", "stackable.stack_type": "stacked"})

    c_idf_scatter = make_card(client, db_id,
        f"Jeunes adultes vs prix au m2 ({Y})", "scatter",
        f"""SELECT nom_commune, zone_idf, prix_m2_median,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND part_25_39 IS NOT NULL
ORDER BY prix_m2_median DESC""",
        "Chaque point = une commune. Les communes cheres ont-elles moins de jeunes ?",
        viz={"graph.dimensions": ["prix_m2_median"], "graph.metrics": ["pct_25_39"],
             "graph.x_axis.title_text": "Prix m2 (EUR)", "graph.y_axis.title_text": "% 25-39 ans"})

    c_idf_ratio = make_card(client, db_id,
        "Ratio prix / revenu par zone (evolution)", "line",
        """SELECT annee::text as annee, zone_idf,
       round(ratio_achat_revenu_pondere::numeric, 1) as ratio_achat
FROM mart_immo__synthese_zone ORDER BY annee, zone_idf""",
        "Annees de revenu pour un achat median.",
        viz={"graph.dimensions": ["annee", "zone_idf"], "graph.metrics": ["ratio_achat"],
             "graph.y_axis.title_text": "Annees de revenu"})

    c_idf_surface = make_card(client, db_id,
        "Surface mediane par zone (evolution)", "line",
        """SELECT annee::text as annee, zone_idf,
       round(sum(surface_mediane * nb_ventes) / nullif(sum(nb_ventes), 0)) as surface_m2
FROM mart_immo__accessibilite_commune
GROUP BY annee, zone_idf ORDER BY annee, zone_idf""",
        "Ce qu'on achete pour le prix.",
        viz={"graph.dimensions": ["annee", "zone_idf"], "graph.metrics": ["surface_m2"],
             "graph.y_axis.title_text": "Surface (m2)"})

    # ── Paris tab cards ──
    c_paris_map_prix = make_card(client, db_id,
        "Prix au m2 — Paris", "map",
        f"SELECT code_commune, nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND code_commune LIKE '751%'",
        viz=map_viz("paris_arr", "prix_m2"))

    c_paris_map_age = make_card(client, db_id,
        "Part des 25-39 ans — Paris", "map",
        f"SELECT code_commune, nom_commune, round((part_25_39 * 100)::numeric, 1) as pct_25_39\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND code_commune LIKE '751%'",
        viz=map_viz("paris_arr", "pct_25_39"))

    c_paris_evol = make_card(client, db_id,
        "Evolution prix au m2 par arrondissement", "line",
        """SELECT annee::text as annee, nom_commune, round(prix_m2_median::numeric) as prix_m2
FROM mart_immo__evolution_prix WHERE code_commune LIKE '751%'
ORDER BY annee, nom_commune""",
        "Prix median au m2 par arrondissement (2020-2025).",
        viz={"graph.dimensions": ["annee", "nom_commune"], "graph.metrics": ["prix_m2"],
             "graph.y_axis.title_text": "EUR / m2"})

    c_paris_table = make_card(client, db_id,
        f"Detail par arrondissement ({Y})", "table",
        f"""SELECT nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(niveau_vie_median::numeric) as revenu, round(surface_mediane::numeric, 1) as surface_m2
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY prix_m2_median DESC""",
        "Prix, ventes, demographie et surface par arrondissement.")

    c_paris_surface = make_card(client, db_id,
        f"Surface mediane par arrondissement ({Y})", "bar",
        f"""SELECT nom_commune, round(surface_mediane::numeric, 1) as surface_m2, nb_ventes
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY surface_mediane DESC""",
        viz={"graph.dimensions": ["nom_commune"], "graph.metrics": ["surface_m2"],
             "graph.y_axis.title_text": "Surface (m2)"})

    # ── Petite couronne tab cards ──
    c_pc_map_prix = make_card(client, db_id,
        "Prix au m2 — Petite couronne", "map",
        f"SELECT code_commune, nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND zone_idf = 'Petite couronne'",
        viz=map_viz("petite_couronne", "prix_m2"))

    c_pc_map_age = make_card(client, db_id,
        "Part des 25-39 ans — Petite couronne", "map",
        f"SELECT code_commune, nom_commune, round((part_25_39 * 100)::numeric, 1) as pct_25_39\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND zone_idf = 'Petite couronne'",
        viz=map_viz("petite_couronne", "pct_25_39"))

    c_pc_evol = make_card(client, db_id,
        "Evolution prix m2 — Petite couronne (top 10)", "line",
        f"""SELECT e.annee::text as annee, e.nom_commune, round(e.prix_m2_median::numeric) as prix_m2
FROM mart_immo__evolution_prix e
WHERE e.code_departement IN ('92','93','94')
  AND e.code_commune IN (
    SELECT code_commune FROM mart_immo__accessibilite_commune
    WHERE zone_idf = 'Petite couronne' AND annee = {Y}
    ORDER BY nb_ventes DESC LIMIT 10)
ORDER BY annee, nom_commune""",
        "10 communes avec le plus de ventes.",
        viz={"graph.dimensions": ["annee", "nom_commune"], "graph.metrics": ["prix_m2"],
             "graph.y_axis.title_text": "EUR / m2"})

    c_pc_table = make_card(client, db_id,
        f"Top 20 communes — Petite couronne ({Y})", "table",
        f"""SELECT nom_commune, code_departement as dept,
       round(prix_m2_median::numeric) as prix_m2, nb_ventes,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(niveau_vie_median::numeric) as revenu,
       round(ratio_achat_revenu_annuel::numeric, 1) as ratio_achat
FROM mart_immo__accessibilite_commune
WHERE zone_idf = 'Petite couronne' AND annee = {Y}
ORDER BY nb_ventes DESC LIMIT 20""",
        "Classees par volume de ventes.")

    # ── Create dashboard with tabs ──
    r = client.post("/api/dashboard", json={
        "name": "Immobilier Ile-de-France",
        "description": "Prix, volumes de ventes et demographie par commune (2020-2025).",
    })
    r.raise_for_status()
    dash_id = r.json()["id"]
    print(f"\n  Dashboard créé (id={dash_id})")

    # Create tabs + cards in one PUT
    r = client.put(f"/api/dashboard/{dash_id}", json={
        "tabs": [
            {"id": -1, "name": "Ile-de-France"},
            {"id": -2, "name": "Paris"},
            {"id": -3, "name": "Petite couronne"},
        ],
        "dashcards": [
            # ── IDF tab (24-col grid) ──
            {"id": -1,  "card_id": c_idf_map_prix,  "dashboard_tab_id": -1, "row": 0,  "col": 0,  "size_x": 12, "size_y": 10},
            {"id": -2,  "card_id": c_idf_map_age,   "dashboard_tab_id": -1, "row": 0,  "col": 12, "size_x": 12, "size_y": 10},
            {"id": -3,  "card_id": c_idf_synthese,   "dashboard_tab_id": -1, "row": 10, "col": 0,  "size_x": 24, "size_y": 7},
            {"id": -4,  "card_id": c_idf_prix,       "dashboard_tab_id": -1, "row": 17, "col": 0,  "size_x": 12, "size_y": 8},
            {"id": -5,  "card_id": c_idf_volume,     "dashboard_tab_id": -1, "row": 17, "col": 12, "size_x": 12, "size_y": 8},
            {"id": -6,  "card_id": c_idf_scatter,    "dashboard_tab_id": -1, "row": 25, "col": 0,  "size_x": 24, "size_y": 10},
            {"id": -7,  "card_id": c_idf_ratio,      "dashboard_tab_id": -1, "row": 35, "col": 0,  "size_x": 12, "size_y": 8},
            {"id": -8,  "card_id": c_idf_surface,    "dashboard_tab_id": -1, "row": 35, "col": 12, "size_x": 12, "size_y": 8},
            # ── Paris tab ──
            {"id": -9,  "card_id": c_paris_map_prix, "dashboard_tab_id": -2, "row": 0,  "col": 0,  "size_x": 12, "size_y": 10},
            {"id": -10, "card_id": c_paris_map_age,  "dashboard_tab_id": -2, "row": 0,  "col": 12, "size_x": 12, "size_y": 10},
            {"id": -11, "card_id": c_paris_evol,     "dashboard_tab_id": -2, "row": 10, "col": 0,  "size_x": 24, "size_y": 9},
            {"id": -12, "card_id": c_paris_table,    "dashboard_tab_id": -2, "row": 19, "col": 0,  "size_x": 24, "size_y": 8},
            {"id": -13, "card_id": c_paris_surface,  "dashboard_tab_id": -2, "row": 27, "col": 0,  "size_x": 24, "size_y": 8},
            # ── Petite couronne tab ──
            {"id": -14, "card_id": c_pc_map_prix,    "dashboard_tab_id": -3, "row": 0,  "col": 0,  "size_x": 12, "size_y": 10},
            {"id": -15, "card_id": c_pc_map_age,     "dashboard_tab_id": -3, "row": 0,  "col": 12, "size_x": 12, "size_y": 10},
            {"id": -16, "card_id": c_pc_evol,        "dashboard_tab_id": -3, "row": 10, "col": 0,  "size_x": 24, "size_y": 9},
            {"id": -17, "card_id": c_pc_table,       "dashboard_tab_id": -3, "row": 19, "col": 0,  "size_x": 24, "size_y": 8},
        ],
    })
    r.raise_for_status()
    print("  3 onglets, 17 cartes")
    return dash_id


# ── Main ────────────────────────────────────────────────────────────


def main() -> None:
    print("\n═══ Setup Metabase — France Aujourd'hui ═══\n")

    print("[1/7] Démarrage PostgreSQL + Metabase")
    start_services()

    print("\n[2/7] Export des marts vers PostgreSQL")
    export_marts_to_postgres()

    print("\n[3/7] Génération GeoJSON")
    generate_geojson()

    client = httpx.Client(base_url=METABASE_URL, timeout=30)

    print("\n[4/7] Connexion à Metabase")
    wait_for_metabase(client)

    print("\n[5/7] Configuration admin")
    session_id = setup_admin(client)
    client.headers["X-Metabase-Session"] = session_id

    print("\n[6/7] Base de données PostgreSQL")
    db_id = add_postgres_database(client)

    print("\n[7/7] Cartes GeoJSON + dashboard")
    register_geojson_maps(client)
    dash_id = create_tabbed_dashboard(client, db_id)

    print(f"\n{'═' * 55}")
    print(f"  Metabase : {METABASE_URL}")
    print(f"  Dashboard : {METABASE_URL}/dashboard/{dash_id}")
    print(f"  Login : {ADMIN_EMAIL} / {ADMIN_PASSWORD}")
    print(f"  3 onglets : Ile-de-France | Paris | Petite couronne")
    print(f"{'═' * 55}\n")

    client.close()


if __name__ == "__main__":
    main()
