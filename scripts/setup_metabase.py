"""Setup Metabase with PostgreSQL and pre-configured tabbed dashboard.

Usage:
    uv run python scripts/setup_metabase.py

Exports dbt mart tables from DuckDB to PostgreSQL, generates GeoJSON maps,
then configures Metabase with 3-tab dashboard (IDF, Paris, Petite couronne).
"""

import json
import subprocess
import sys
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
    "main_marts.mart_immo__synthese_zone",
    "main_marts.mart_immo__evolution_prix",
    ("main_staging.stg_logement__delinquance_detail", "stg_delinquance_detail"),
    ("""SELECT code_commune, nom_commune,
    cast(nb_pieces as integer) as nb_pieces, cast(annee as integer) as annee,
    count(*) as nb_ventes,
    round(median(valeur_fonciere)) as prix_median,
    round(median(valeur_fonciere / nullif(surface_bati, 0))) as prix_m2_median,
    round(median(surface_bati), 1) as surface_mediane
FROM main_staging.stg_dvf__mutations_idf
WHERE type_local = 'Appartement' AND nb_pieces IN (1, 2, 3)
  AND valeur_fonciere > 10000 AND code_commune LIKE '751%'
GROUP BY code_commune, nom_commune, nb_pieces, annee
HAVING count(*) >= 5""", "prix_paris_par_pieces"),
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
    con = duckdb.connect(str(MAIN_DB_PATH))
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")

    for table in MART_TABLES:
        if isinstance(table, tuple):
            source, short_name = table
        else:
            source, short_name = table, table.split(".")[-1]
        con.execute(f"DROP TABLE IF EXISTS pg.public.{short_name}")
        if source.lstrip().upper().startswith("SELECT"):
            con.execute(f"CREATE TABLE pg.public.{short_name} AS {source}")
        else:
            con.execute(f"CREATE TABLE pg.public.{short_name} AS SELECT * FROM {source}")
        rows = con.execute(f"SELECT count(*) FROM pg.public.{short_name}").fetchone()[0]
        print(f"  {short_name} — {rows:,} lignes")

    con.execute("DETACH pg")
    con.close()


def export_velib_to_postgres() -> None:
    """Download Vélib station data and aggregate by commune via spatial join."""
    r = httpx.get(
        "https://velib-metropole-opendata.smovengo.cloud/opendata/Velib_Metropole/station_information.json",
        timeout=30,
    )
    stations = r.json()["data"]["stations"]
    print(f"  {len(stations)} stations Vélib téléchargées")

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")

    con.execute("""CREATE TABLE stations (
        code VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE, capacity INTEGER)""")
    con.executemany(
        "INSERT INTO stations VALUES (?, ?, ?, ?, ?)",
        [(str(s.get("stationCode", s["station_id"])), s["name"],
          s["lat"], s["lon"], s["capacity"]) for s in stations],
    )

    geojson_path = str(GEOJSON_DIR / "idf_communes.geojson")
    con.execute(f"CREATE TABLE geo AS SELECT * FROM ST_Read('{geojson_path}')")

    con.execute("DROP TABLE IF EXISTS pg.public.velib_stations")
    con.execute("""
        CREATE TABLE pg.public.velib_stations AS
        WITH areas AS (
            SELECT code, nom, ST_Area(geom) * 111.12 * 111.12 * 0.6583 as area_km2
            FROM geo
        ),
        counts AS (
            SELECT g.code as code_commune, g.nom as nom_commune,
                   count(*)::integer as nb_stations,
                   sum(s.capacity)::integer as capacite_totale
            FROM stations s
            JOIN geo g ON ST_Contains(g.geom, ST_Point(s.lon, s.lat))
            GROUP BY g.code, g.nom
        )
        SELECT c.code_commune, c.nom_commune,
               c.nb_stations, c.capacite_totale,
               round(a.area_km2::numeric, 2) as superficie_km2,
               round((c.nb_stations / a.area_km2)::numeric, 1) as stations_par_km2
        FROM counts c JOIN areas a ON c.code_commune = a.code
        ORDER BY c.code_commune
    """)
    rows = con.execute("SELECT count(*) FROM pg.public.velib_stations").fetchone()[0]
    print(f"  velib_stations — {rows} communes")

    con.execute("DROP TABLE IF EXISTS pg.public.velib_stations_geo")
    con.execute("""
        CREATE TABLE pg.public.velib_stations_geo AS
        SELECT s.name, s.lat, s.lon, s.capacity,
               g.code as code_commune, g.nom as nom_commune
        FROM stations s
        JOIN geo g ON ST_Contains(g.geom, ST_Point(s.lon, s.lat))
    """)
    geo_rows = con.execute("SELECT count(*) FROM pg.public.velib_stations_geo").fetchone()[0]
    print(f"  velib_stations_geo — {geo_rows} stations géolocalisées")

    con.execute("DETACH pg")
    con.close()


def export_cycling_to_postgres() -> None:
    """Download Paris cycling infrastructure and counter data."""
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")

    # Compute arrondissement areas from GeoJSON
    geojson_path = str(GEOJSON_DIR / "paris_arrondissements.geojson")
    con.execute(f"CREATE TABLE geo AS SELECT * FROM ST_Read('{geojson_path}')")
    con.execute("""CREATE TABLE areas AS
        SELECT code, ST_Area(geom) * 111.12 * 111.12 * 0.6583 as area_km2 FROM geo""")

    # Cycling infrastructure km by arrondissement (normalized by area)
    r = httpx.get(
        "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
        "amenagements-cyclables/records",
        params={"select": "arrondissement, sum(st_length_shape) as longueur_m",
                "group_by": "arrondissement", "limit": 25},
        timeout=15,
    )
    infra = r.json().get("results", [])
    con.execute("CREATE TABLE infra (arrondissement INTEGER, longueur_km DOUBLE)")
    con.executemany("INSERT INTO infra VALUES (?, ?)",
                    [(int(d["arrondissement"]), round(d["longueur_m"] / 1000, 1))
                     for d in infra])

    con.execute("DROP TABLE IF EXISTS pg.public.cyclable_paris")
    con.execute("""CREATE TABLE pg.public.cyclable_paris AS
        SELECT '751' || lpad(i.arrondissement::text, 2, '0') as code_commune,
               g.nom as nom_commune,
               i.arrondissement, i.longueur_km,
               round((i.longueur_km / a.area_km2)::numeric, 1) as km_par_km2
        FROM infra i
        JOIN areas a ON a.code = '751' || lpad(i.arrondissement::text, 2, '0')
        JOIN geo g ON g.code = a.code
        ORDER BY i.arrondissement""")
    print(f"  cyclable_paris — {len(infra)} arrondissements")

    # Top bike counters with coordinates (latest full month)
    # 1) Get counter locations
    r_loc = httpx.get(
        "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
        "comptage-velo-compteurs/records",
        params={"limit": 100}, timeout=15,
    )
    loc_data = {d["id_compteur"]: d.get("coordinates", {})
                for d in r_loc.json().get("results", [])}

    # 2) Get monthly traffic
    r2 = httpx.get(
        "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
        "comptage-velo-donnees-compteurs/records",
        params={"select": "id_compteur, nom_compteur, sum(sum_counts) as total_passages",
                "where": 'date >= "2026-03-01" and date < "2026-04-01"',
                "group_by": "id_compteur, nom_compteur",
                "order_by": "total_passages desc", "limit": 30},
        timeout=15,
    )
    counters = r2.json().get("results", [])

    con.execute("CREATE TABLE counters (nom VARCHAR, lat DOUBLE, lon DOUBLE, total_passages INTEGER)")
    con.executemany("INSERT INTO counters VALUES (?, ?, ?, ?)",
                    [(d["nom_compteur"],
                      loc_data.get(d["id_compteur"], {}).get("lat"),
                      loc_data.get(d["id_compteur"], {}).get("lon"),
                      d["total_passages"]) for d in counters])

    con.execute("DROP TABLE IF EXISTS pg.public.comptage_velo_paris")
    con.execute("""CREATE TABLE pg.public.comptage_velo_paris AS
        SELECT * FROM counters WHERE lat IS NOT NULL ORDER BY total_passages DESC""")
    rows = con.execute("SELECT count(*) FROM pg.public.comptage_velo_paris").fetchone()[0]
    print(f"  comptage_velo_paris — {rows} compteurs géolocalisés")

    con.execute("DETACH pg")
    con.close()


def export_diplomes_to_postgres() -> None:
    """Download INSEE diploma data and compute education metrics by commune."""
    import io
    import zipfile

    r = httpx.get(
        "https://www.insee.fr/fr/statistiques/fichier/8202319/"
        "base-cc-diplomes-formation-2021_csv.zip",
        timeout=60, follow_redirects=True,
    )
    z = zipfile.ZipFile(io.BytesIO(r.content))
    tmp = PROJECT_ROOT / "data" / "raw" / "diplomes_formation_2021.csv"
    with z.open("base-cc-diplomes-formation-2021.CSV") as src, open(tmp, "wb") as dst:
        dst.write(src.read())
    print("  Diplômes RP 2021 téléchargés")

    con = duckdb.connect()
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")

    con.execute("DROP TABLE IF EXISTS pg.public.diplomes_communes")
    con.execute(f"""
        CREATE TABLE pg.public.diplomes_communes AS
        SELECT
            "CODGEO" as code_commune,
            round(cast("P21_NSCOL15P" as double)) as pop_15p_non_scol,
            round(cast("P21_NSCOL15P_DIPLMIN" as double)
                  / nullif(cast("P21_NSCOL15P" as double), 0) * 100, 1) as part_sans_diplome,
            round((cast("P21_NSCOL15P_SUP2" as double)
                 + cast("P21_NSCOL15P_SUP34" as double)
                 + cast("P21_NSCOL15P_SUP5" as double))
                  / nullif(cast("P21_NSCOL15P" as double), 0) * 100, 1) as part_etudes_sup,
            round(cast("P21_NSCOL15P_SUP5" as double)
                  / nullif(cast("P21_NSCOL15P" as double), 0) * 100, 1) as part_bac5_plus
        FROM read_csv('{tmp}', delim=';', header=true, auto_detect=true)
        WHERE cast("P21_NSCOL15P" as double) > 0
    """)
    rows = con.execute("SELECT count(*) FROM pg.public.diplomes_communes").fetchone()[0]
    print(f"  diplomes_communes — {rows:,} communes")

    con.execute("DETACH pg")
    con.close()


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
    if r.status_code == 403:
        print("  Setup déjà fait, connexion…")
        r = client.post("/api/session", json={
            "username": ADMIN_EMAIL, "password": ADMIN_PASSWORD,
        })
        r.raise_for_status()
        return r.json()["id"]
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


# Single alternative color for right-side maps (light green)
CLR_ALT = ["#E8F5E9", "#C8E6C9", "#A5D6A7", "#81C784", "#66BB6A"]


def map_viz(region: str, metric: str, colors: list[str] | None = None) -> dict:
    viz = {"map.type": "region", "map.region": region,
           "map.dimension": "code_commune", "map.metric": metric}
    if colors:
        viz["map.colors"] = colors
    return viz


# ── Dashboard creation ──────────────────────────────────────────────

Y = str(LATEST_YEAR)

DELIN_CATEGORIES = """\
CASE
    WHEN d.type_delit LIKE 'Vols%' THEN 'Vols'
    WHEN d.type_delit LIKE '%stupéfiants%' THEN 'Stupéfiants'
    WHEN d.type_delit LIKE 'Violences%' THEN 'Violences'
    WHEN d.type_delit IN ('Cambriolages de logement',
         'Destructions et dégradations volontaires') THEN 'Atteintes aux biens'
    ELSE 'Escroqueries'
END"""


def _heading(id_: int, tab: int, row: int, text: str) -> dict:
    """Dashboard section heading."""
    return {"id": id_, "card_id": None, "dashboard_tab_id": tab,
            "row": row, "col": 0, "size_x": 24, "size_y": 2,
            "visualization_settings": {
                "virtual_card": {"name": None, "display": "text", "archived": False},
                "text": f"## {text}",
                "dashcard.background": False,
                "text.align_vertical": "bottom",
                "text.align_horizontal": "center"}}


def create_tabbed_dashboard(client: httpx.Client, db_id: int) -> int:
    """Create (or replace) the full 3-tab dashboard with all cards."""

    # ── Find or create dashboard (keep exactly one) ──
    dash_id = None
    r = client.get("/api/search", params={"models": "dashboard", "limit": 50})
    for item in r.json().get("data", []):
        if dash_id is None and item.get("name") == "Panorama Ile-de-France":
            dash_id = item["id"]
            print(f"  Dashboard existant (id={dash_id}), mise a jour…")
        else:
            client.delete(f"/api/dashboard/{item['id']}")
            print(f"  Ancien dashboard {item['id']} supprime")

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
        "Chaque point = une commune.",
        viz={"graph.dimensions": ["prix_m2_median"], "graph.metrics": ["pct_25_39"],
             "graph.x_axis.title_text": "Prix m2 (EUR)", "graph.y_axis.title_text": "% 25-39 ans"})

    c_idf_ratio = make_card(client, db_id,
        "Ratio prix / revenu par zone", "line",
        """SELECT annee::text as annee, zone_idf,
       round(ratio_achat_revenu_pondere::numeric, 1) as ratio_achat
FROM mart_immo__synthese_zone ORDER BY annee, zone_idf""",
        "Annees de revenu pour un achat median.",
        viz={"graph.dimensions": ["annee", "zone_idf"], "graph.metrics": ["ratio_achat"],
             "graph.y_axis.title_text": "Annees de revenu"})

    c_idf_surface = make_card(client, db_id,
        "Surface mediane par zone", "line",
        """SELECT annee::text as annee, zone_idf,
       round(sum(surface_mediane * nb_ventes) / nullif(sum(nb_ventes), 0)) as surface_m2
FROM mart_immo__accessibilite_commune
GROUP BY annee, zone_idf ORDER BY annee, zone_idf""",
        viz={"graph.dimensions": ["annee", "zone_idf"], "graph.metrics": ["surface_m2"],
             "graph.y_axis.title_text": "Surface (m2)"})

    c_idf_map_loyer = make_card(client, db_id,
        "Loyer au m2 — Ile-de-France", "map",
        f"SELECT code_commune, nom_commune, round(loyer_m2_median::numeric, 1) as loyer_m2, zone_idf\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND loyer_m2_median IS NOT NULL",
        viz=map_viz("idf_communes", "loyer_m2"))

    c_idf_loyer_vs_achat = make_card(client, db_id,
        "Loyer vs prix d'achat au m2 par zone", "bar",
        f"""SELECT zone_idf,
       round((sum(loyer_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0))::numeric, 1) as loyer_m2_mois,
       round((sum(prix_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0) / 12)::numeric, 0) as mensualite_achat_m2
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND loyer_m2_median IS NOT NULL
GROUP BY zone_idf ORDER BY zone_idf""",
        viz={"graph.dimensions": ["zone_idf"],
             "graph.metrics": ["loyer_m2_mois", "mensualite_achat_m2"],
             "graph.y_axis.title_text": "EUR / m2"})

    c_idf_map_delin = make_card(client, db_id,
        "Delinquance — Ile-de-France", "map",
        f"""SELECT code_commune, nom_commune,
       round(taux_delinquance_pour_mille::numeric, 1) as taux_delin, zone_idf
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND taux_delinquance_pour_mille IS NOT NULL""",
        viz=map_viz("idf_communes", "taux_delin"))

    c_idf_evol_delin = make_card(client, db_id,
        "Evolution delinquance par zone", "line",
        """SELECT annee::text as annee, zone_idf,
       round((sum(nb_faits_delinquance::numeric) / nullif(sum(population_2021), 0) * 1000)::numeric, 1) as taux_delin
FROM mart_immo__accessibilite_commune
WHERE taux_delinquance_pour_mille IS NOT NULL
GROUP BY annee, zone_idf ORDER BY annee, zone_idf""",
        viz={"graph.dimensions": ["annee", "zone_idf"], "graph.metrics": ["taux_delin"],
             "graph.y_axis.title_text": "Faits / 1000 hab."})

    c_idf_delin_type = make_card(client, db_id,
        "Delinquance par categorie — Ile-de-France", "bar",
        f"""SELECT {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux_pour_mille
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE d.annee = 2024 AND a.zone_idf IS NOT NULL
GROUP BY categorie ORDER BY taux_pour_mille DESC""",
        viz={"graph.dimensions": ["categorie"], "graph.metrics": ["taux_pour_mille"],
             "graph.y_axis.title_text": "Faits / 1000 hab."})

    c_idf_evol_delits = make_card(client, db_id,
        "Evolution des delits par categorie — IDF", "line",
        f"""SELECT d.annee::text as annee, {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE a.zone_idf IS NOT NULL
GROUP BY d.annee, categorie ORDER BY d.annee, categorie""",
        viz={"graph.dimensions": ["annee", "categorie"], "graph.metrics": ["taux"],
             "graph.y_axis.title_text": "Faits / 1000 hab."})

    c_idf_map_diplome = make_card(client, db_id,
        "Part des diplômés du supérieur — Ile-de-France", "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_etudes_sup, a.zone_idf
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND d.part_etudes_sup IS NOT NULL""",
        viz=map_viz("idf_communes", "part_etudes_sup"))

    c_idf_map_sans_diplome = make_card(client, db_id,
        "Part sans diplôme — Ile-de-France", "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_sans_diplome, a.zone_idf
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND d.part_sans_diplome IS NOT NULL""",
        viz=map_viz("idf_communes", "part_sans_diplome"))

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
        viz={"graph.dimensions": ["annee", "nom_commune"], "graph.metrics": ["prix_m2"],
             "graph.y_axis.title_text": "EUR / m2"})

    c_paris_table = make_card(client, db_id,
        f"Detail par arrondissement ({Y})", "table",
        f"""SELECT nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(niveau_vie_median::numeric) as revenu, round(surface_mediane::numeric, 1) as surface_m2
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY prix_m2_median DESC""")

    c_paris_surface = make_card(client, db_id,
        f"Surface mediane par arrondissement ({Y})", "bar",
        f"""SELECT nom_commune, round(surface_mediane::numeric, 1) as surface_m2, nb_ventes
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY surface_mediane DESC""",
        viz={"graph.dimensions": ["nom_commune"], "graph.metrics": ["surface_m2"],
             "graph.y_axis.title_text": "Surface (m2)"})

    c_paris_map_loyer_studio = make_card(client, db_id,
        "Loyer estime studio — Paris", "map",
        f"""SELECT a.code_commune, a.nom_commune,
       round((a.loyer_m2_median * p.surface_mediane)::numeric) as loyer_mensuel
FROM mart_immo__accessibilite_commune a
JOIN prix_paris_par_pieces p
    ON a.code_commune = p.code_commune AND a.annee = p.annee
WHERE a.annee = {Y} AND p.nb_pieces = 1 AND a.loyer_m2_median IS NOT NULL""",
        "Loyer m2 x surface mediane studio (carte des loyers 2025).",
        map_viz("paris_arr", "loyer_mensuel"))

    c_paris_map_loyer_2p = make_card(client, db_id,
        "Loyer estimé 2 pièces — Paris", "map",
        f"""SELECT a.code_commune, a.nom_commune,
       round((a.loyer_m2_median * p.surface_mediane)::numeric) as loyer_mensuel
FROM mart_immo__accessibilite_commune a
JOIN prix_paris_par_pieces p
    ON a.code_commune = p.code_commune AND a.annee = p.annee
WHERE a.annee = {Y} AND p.nb_pieces = 2 AND a.loyer_m2_median IS NOT NULL""",
        "Loyer m2 x surface médiane 2P (carte des loyers 2025).",
        map_viz("paris_arr", "loyer_mensuel"))

    c_paris_map_delin = make_card(client, db_id,
        "Delinquance — Paris", "map",
        f"""SELECT code_commune, nom_commune,
       round(taux_delinquance_pour_mille::numeric, 1) as taux_delin
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND code_commune LIKE '751%' AND taux_delinquance_pour_mille IS NOT NULL""",
        viz=map_viz("paris_arr", "taux_delin"))

    c_paris_delin_cat = make_card(client, db_id,
        "Delinquance par categorie et arrondissement", "bar",
        f"""SELECT cast(right(d.code_commune, 2) as integer) as arrdt,
       {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux_pour_mille
FROM stg_delinquance_detail d
WHERE d.annee = 2024 AND d.code_commune LIKE '751%'
GROUP BY arrdt, categorie ORDER BY arrdt, taux_pour_mille DESC""",
        viz={"graph.dimensions": ["arrdt", "categorie"],
             "graph.metrics": ["taux_pour_mille"],
             "graph.y_axis.title_text": "Faits / 1000 hab.",
             "stackable.stack_type": "stacked"})

    c_paris_map_velib = make_card(client, db_id,
        "Densité Vélib — Paris (stations/km²)", "map",
        """SELECT code_commune, nom_commune, stations_par_km2
FROM velib_stations WHERE code_commune LIKE '751%'""",
        viz=map_viz("paris_arr", "stations_par_km2"))

    c_paris_map_cyclable = make_card(client, db_id,
        "Densité pistes cyclables — Paris (km/km²)", "map",
        """SELECT code_commune, nom_commune, km_par_km2
FROM cyclable_paris""",
        viz=map_viz("paris_arr", "km_par_km2"))

    c_paris_map_diplome = make_card(client, db_id,
        "Part des diplômés du supérieur — Paris", "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_etudes_sup
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND a.code_commune LIKE '751%'""",
        viz=map_viz("paris_arr", "part_etudes_sup"))

    c_paris_map_sans_diplome = make_card(client, db_id,
        "Part sans diplôme — Paris", "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_sans_diplome
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND a.code_commune LIKE '751%'""",
        viz=map_viz("paris_arr", "part_sans_diplome"))

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
        "Evolution prix m2 — top 10 communes", "line",
        f"""SELECT e.annee::text as annee, e.nom_commune, round(e.prix_m2_median::numeric) as prix_m2
FROM mart_immo__evolution_prix e
WHERE e.code_departement IN ('92','93','94')
  AND e.code_commune IN (
    SELECT code_commune FROM mart_immo__accessibilite_commune
    WHERE zone_idf = 'Petite couronne' AND annee = {Y}
    ORDER BY nb_ventes DESC LIMIT 10)
ORDER BY annee, nom_commune""",
        viz={"graph.dimensions": ["annee", "nom_commune"], "graph.metrics": ["prix_m2"],
             "graph.y_axis.title_text": "EUR / m2"})

    c_pc_table = make_card(client, db_id,
        f"Top 20 communes ({Y})", "table",
        f"""SELECT nom_commune, code_departement as dept,
       round(prix_m2_median::numeric) as prix_m2, nb_ventes,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(niveau_vie_median::numeric) as revenu,
       round(ratio_achat_revenu_annuel::numeric, 1) as ratio_achat
FROM mart_immo__accessibilite_commune
WHERE zone_idf = 'Petite couronne' AND annee = {Y}
ORDER BY nb_ventes DESC LIMIT 20""")

    c_pc_map_loyer = make_card(client, db_id,
        "Loyer au m2 — Petite couronne", "map",
        f"SELECT code_commune, nom_commune, round(loyer_m2_median::numeric, 1) as loyer_m2\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND zone_idf = 'Petite couronne' AND loyer_m2_median IS NOT NULL",
        viz=map_viz("petite_couronne", "loyer_m2"))

    c_pc_map_delin = make_card(client, db_id,
        "Delinquance — Petite couronne", "map",
        f"""SELECT code_commune, nom_commune,
       round(taux_delinquance_pour_mille::numeric, 1) as taux_delin
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND zone_idf = 'Petite couronne' AND taux_delinquance_pour_mille IS NOT NULL""",
        viz=map_viz("petite_couronne", "taux_delin"))

    c_pc_delin_cat = make_card(client, db_id,
        "Delinquance par categorie — Petite couronne", "bar",
        f"""SELECT {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux_pour_mille
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE d.annee = 2024 AND a.zone_idf = 'Petite couronne'
GROUP BY categorie ORDER BY taux_pour_mille DESC""",
        viz={"graph.dimensions": ["categorie"], "graph.metrics": ["taux_pour_mille"],
             "graph.y_axis.title_text": "Faits / 1000 hab."})

    c_pc_map_velib = make_card(client, db_id,
        "Stations Vélib — Petite couronne", "map",
        """SELECT code_commune, nom_commune, nb_stations
FROM velib_stations WHERE left(code_commune, 2) IN ('92', '93', '94')""",
        viz=map_viz("petite_couronne", "nb_stations"))

    c_pc_map_diplome = make_card(client, db_id,
        "Part des diplômés du supérieur — Petite couronne", "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_etudes_sup
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND a.zone_idf = 'Petite couronne'""",
        viz=map_viz("petite_couronne", "part_etudes_sup"))

    # ── Create dashboard if needed ──
    if dash_id is None:
        r = client.post("/api/dashboard", json={
            "name": "Panorama Ile-de-France",
            "description": "Immobilier, revenus, loyers, demographie et securite par commune (2020-2025).",
        })
        r.raise_for_status()
        dash_id = r.json()["id"]
        print(f"\n  Dashboard cree (id={dash_id})")
    print()

    # ── Layout with section headings ──
    T1, T2, T3 = -1, -2, -3  # tab IDs
    n = 0  # auto-increment dashcard IDs

    def _card(card_id, tab, row, col, sx, sy):
        nonlocal n; n -= 1
        return {"id": n, "card_id": card_id, "dashboard_tab_id": tab,
                "row": row, "col": col, "size_x": sx, "size_y": sy}

    def _head(tab, row, text):
        nonlocal n; n -= 1
        return _heading(n, tab, row, text)

    r = client.put(f"/api/dashboard/{dash_id}", json={
        "tabs": [
            {"id": T1, "name": "Ile-de-France"},
            {"id": T2, "name": "Paris"},
            {"id": T3, "name": "Petite couronne"},
        ],
        "dashcards": [
            # ═══ IDF ═══
            _head(T1,  0, "Marché immobilier"),
            _card(c_idf_map_prix,       T1,  2,  0, 12, 10),
            _card(c_idf_map_age,        T1,  2, 12, 12, 10),
            _card(c_idf_synthese,       T1, 12,  0, 24,  7),
            _card(c_idf_prix,           T1, 19,  0, 12,  8),
            _card(c_idf_volume,         T1, 19, 12, 12,  8),
            _card(c_idf_scatter,        T1, 27,  0, 24, 10),
            _head(T1, 38, "Revenus et accessibilité"),
            _card(c_idf_ratio,          T1, 40,  0, 12,  8),
            _card(c_idf_surface,        T1, 40, 12, 12,  8),
            _head(T1, 49, "Loyers"),
            _card(c_idf_map_loyer,      T1, 51,  0, 12, 10),
            _card(c_idf_loyer_vs_achat, T1, 51, 12, 12, 10),
            _head(T1, 62, "Sécurité"),
            _card(c_idf_map_delin,      T1, 64,  0, 12, 10),
            _card(c_idf_evol_delin,     T1, 64, 12, 12, 10),
            _card(c_idf_delin_type,     T1, 74,  0, 24,  8),
            _card(c_idf_evol_delits,    T1, 82,  0, 24,  8),
            _head(T1, 91, "Éducation"),
            _card(c_idf_map_diplome,       T1, 93,  0, 12, 10),
            _card(c_idf_map_sans_diplome,  T1, 93, 12, 12, 10),
            # ═══ Paris ═══
            _head(T2,  0, "Marché immobilier"),
            _card(c_paris_map_prix,     T2,  2,  0, 12, 10),
            _card(c_paris_map_age,      T2,  2, 12, 12, 10),
            _card(c_paris_evol,         T2, 12,  0, 24,  9),
            _card(c_paris_table,        T2, 21,  0, 24,  8),
            _card(c_paris_surface,      T2, 29,  0, 24,  8),
            _head(T2, 38, "Locations"),
            _card(c_paris_map_loyer_studio, T2, 40,  0, 12, 10),
            _card(c_paris_map_loyer_2p,     T2, 40, 12, 12, 10),
            _head(T2, 51, "Sécurité"),
            _card(c_paris_map_delin,    T2, 53,  0, 24, 10),
            _card(c_paris_delin_cat,    T2, 63,  0, 24, 10),
            _head(T2, 74, "Éducation"),
            _card(c_paris_map_diplome,      T2, 76,  0, 12, 10),
            _card(c_paris_map_sans_diplome, T2, 76, 12, 12, 10),
            _head(T2, 87, "Mobilité"),
            _card(c_paris_map_velib,        T2, 89,  0, 12, 10),
            _card(c_paris_map_cyclable,     T2, 89, 12, 12, 10),
            # ═══ Petite couronne ═══
            _head(T3,  0, "Marché immobilier"),
            _card(c_pc_map_prix,        T3,  2,  0, 12, 10),
            _card(c_pc_map_age,         T3,  2, 12, 12, 10),
            _card(c_pc_evol,            T3, 12,  0, 24,  9),
            _card(c_pc_table,           T3, 21,  0, 24,  8),
            _head(T3, 30, "Loyers et sécurité"),
            _card(c_pc_map_loyer,       T3, 32,  0, 12, 10),
            _card(c_pc_map_delin,       T3, 32, 12, 12, 10),
            _card(c_pc_delin_cat,       T3, 42,  0, 24,  8),
            _head(T3, 51, "Éducation"),
            _card(c_pc_map_diplome,     T3, 53,  0, 24, 10),
            _head(T3, 64, "Mobilité"),
            _card(c_pc_map_velib,       T3, 66,  0, 24, 10),
        ],
    })
    r.raise_for_status()
    print("  3 onglets, 41 cartes, 14 sections")
    return dash_id


# ── Main ────────────────────────────────────────────────────────────


def main() -> None:
    print("\n═══ Setup Metabase — France Aujourd'hui ═══\n")

    print("[1/7] Démarrage PostgreSQL + Metabase")
    start_services()

    print("\n[2/8] Export des marts vers PostgreSQL")
    export_marts_to_postgres()

    print("\n[3/8] Génération GeoJSON")
    generate_geojson()

    print("\n[4/10] Données Vélib")
    export_velib_to_postgres()

    print("\n[5/10] Pistes cyclables et comptages vélo")
    export_cycling_to_postgres()

    print("\n[6/10] Données diplômes INSEE")
    export_diplomes_to_postgres()

    client = httpx.Client(base_url=METABASE_URL, timeout=30)

    print("\n[7/10] Connexion à Metabase")
    wait_for_metabase(client)

    print("\n[8/10] Configuration admin")
    session_id = setup_admin(client)
    client.headers["X-Metabase-Session"] = session_id

    print("\n[9/10] Base de données PostgreSQL")
    db_id = add_postgres_database(client)

    print("\n[10/10] Cartes GeoJSON + dashboard")
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
