"""Setup Metabase with PostgreSQL and pre-configured tabbed dashboard.

Usage:
    uv run python scripts/setup_metabase.py

Exports dbt mart tables from DuckDB to PostgreSQL, generates GeoJSON maps,
then configures Metabase with 3-tab dashboard (IDF, Paris, Petite couronne).
"""

import json
import os
import shutil
import subprocess
import sys
import time
from pathlib import Path

import duckdb
import httpx

PROJECT_ROOT = Path(__file__).resolve().parent.parent
MAIN_DB_PATH = PROJECT_ROOT / "data" / "panorama_idf.duckdb"
GEOJSON_DIR = PROJECT_ROOT / "data" / "metabase"
GEOJSON_NGINX_CONF = GEOJSON_DIR / "nginx.conf"


def _load_dotenv(path: Path) -> None:
    """Minimal .env loader (no external dependency). Ne remplace pas les vars deja definies."""
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        os.environ.setdefault(key, value)


_load_dotenv(PROJECT_ROOT / ".env")


# ── Config (env-first, defauts dev) ─────────────────────────────────
METABASE_URL = os.environ.get("METABASE_URL", "http://localhost:3000")
ADMIN_EMAIL = os.environ.get("MB_ADMIN_EMAIL", "admin@panorama-idf.local")
ADMIN_PASSWORD = os.environ.get("MB_ADMIN_PASSWORD", "PanoramaIdf2024!")

# PG_CONN : soit explicitement fourni, soit assemble depuis les parts POSTGRES_*.
_pg_host = os.environ.get("POSTGRES_HOST", "localhost")
_pg_port = os.environ.get("POSTGRES_PORT", "5480")
_pg_db = os.environ.get("POSTGRES_DB", "panorama_idf")
_pg_user = os.environ.get("POSTGRES_USER", "metabase")
_pg_password = os.environ.get("POSTGRES_PASSWORD", "metabase")
PG_CONN = os.environ.get(
    "PG_CONN",
    f"host={_pg_host} port={_pg_port} dbname={_pg_db} user={_pg_user} password={_pg_password}",
)

# En prod on passe COMPOSE_FILE=docker-compose.prod.yml pour que start_services()
# lance la bonne stack. Par defaut = docker-compose.yml (dev).
COMPOSE_FILE = os.environ.get("COMPOSE_FILE", "docker-compose.yml")

_geojson_base_url = os.environ.get("GEOJSON_BASE_URL")
if not _geojson_base_url:
    _domain = os.environ.get("DOMAIN", "").strip()
    if _domain:
        _geojson_base_url = f"https://{_domain}/geojson"
    elif COMPOSE_FILE == "docker-compose.yml":
        _geojson_base_url = "http://geojson:80"
    else:
        _geojson_base_url = f"{METABASE_URL.rstrip('/')}/geojson"
GEOJSON_BASE_URL = _geojson_base_url.rstrip("/")

MART_TABLES = [
    "main_marts.mart_immo__accessibilite_commune",
    "main_marts.mart_immo__synthese_zone",
    "main_marts.mart_immo__evolution_prix",
    ("main_staging.stg_logement__delinquance_detail", "stg_delinquance_detail"),
    (
        """SELECT code_commune, nom_commune,
    cast(nb_pieces as integer) as nb_pieces, cast(annee as integer) as annee,
    count(*) as nb_ventes,
    round(median(valeur_fonciere)) as prix_median,
    round(median(valeur_fonciere / nullif(surface_bati, 0))) as prix_m2_median,
    round(median(surface_bati), 1) as surface_mediane
FROM main_staging.stg_dvf__mutations_idf
WHERE type_local = 'Appartement' AND nb_pieces IN (1, 2, 3)
  AND valeur_fonciere > 10000 AND code_commune LIKE '751%'
GROUP BY code_commune, nom_commune, nb_pieces, annee
HAVING count(*) >= 5""",
        "prix_paris_par_pieces",
    ),
]

LATEST_YEAR = 2025


# ── Infrastructure ──────────────────────────────────────────────────


def _compose_cmd(*args: str) -> list[str]:
    """Build a `docker compose -f <file> ...` command using the configured compose file."""
    if shutil.which("docker-compose"):
        return ["docker-compose", "-f", COMPOSE_FILE, *args]
    return ["docker", "compose", "-f", COMPOSE_FILE, *args]


def start_services() -> None:
    """Start PostgreSQL and Metabase via docker compose."""
    subprocess.run(_compose_cmd("up", "-d"), cwd=PROJECT_ROOT, check=True)
    print("  Attente de PostgreSQL…", end="", flush=True)
    for _ in range(30):
        result = subprocess.run(
            _compose_cmd(
                "exec", "-T", "postgres",
                "pg_isready", "-U", _pg_user, "-d", _pg_db,
            ),
            cwd=PROJECT_ROOT,
            capture_output=True,
        )
        if result.returncode == 0:
            print(" OK")
            subprocess.run(
                _compose_cmd(
                    "exec", "-T", "postgres",
                    "psql", "-U", _pg_user, "-d", _pg_db,
                    "-c", f"CREATE DATABASE metabase_app OWNER {_pg_user};",
                ),
                cwd=PROJECT_ROOT,
                capture_output=True,
            )
            return
        print(".", end="", flush=True)
        time.sleep(2)
    print(" TIMEOUT")
    sys.exit(1)


def ensure_geojson_nginx_config() -> None:
    """Write the nginx config used to serve GeoJSON with the correct MIME type."""
    GEOJSON_DIR.mkdir(parents=True, exist_ok=True)
    GEOJSON_NGINX_CONF.write_text(
        """server {
    listen 80;
    server_name _;
    root /usr/share/nginx/html;

    types {
        application/geo+json geojson;
        application/json json;
    }

    location / {
        add_header Access-Control-Allow-Origin *;
        try_files $uri =404;
    }
}
""",
        encoding="utf-8",
    )


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
            con.execute(
                f"CREATE TABLE pg.public.{short_name} AS SELECT * FROM {source}"
            )
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

    con.execute(
        """CREATE TABLE stations (
        code VARCHAR, name VARCHAR, lat DOUBLE, lon DOUBLE, capacity INTEGER)"""
    )
    con.executemany(
        "INSERT INTO stations VALUES (?, ?, ?, ?, ?)",
        [
            (
                str(s.get("stationCode", s["station_id"])),
                s["name"],
                s["lat"],
                s["lon"],
                s["capacity"],
            )
            for s in stations
        ],
    )

    geojson_path = str(GEOJSON_DIR / "idf_communes.geojson")
    con.execute(f"CREATE TABLE geo AS SELECT * FROM ST_Read('{geojson_path}')")

    con.execute("DROP TABLE IF EXISTS pg.public.velib_stations")
    con.execute(
        """
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
    """
    )
    rows = con.execute("SELECT count(*) FROM pg.public.velib_stations").fetchone()[0]
    print(f"  velib_stations — {rows} communes")

    con.execute("DROP TABLE IF EXISTS pg.public.velib_stations_geo")
    con.execute(
        """
        CREATE TABLE pg.public.velib_stations_geo AS
        SELECT s.name, s.lat, s.lon, s.capacity,
               g.code as code_commune, g.nom as nom_commune
        FROM stations s
        JOIN geo g ON ST_Contains(g.geom, ST_Point(s.lon, s.lat))
    """
    )
    geo_rows = con.execute(
        "SELECT count(*) FROM pg.public.velib_stations_geo"
    ).fetchone()[0]
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
    con.execute(
        """CREATE TABLE areas AS
        SELECT code, ST_Area(geom) * 111.12 * 111.12 * 0.6583 as area_km2 FROM geo"""
    )

    # Cycling infrastructure km by arrondissement (normalized by area)
    r = httpx.get(
        "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
        "amenagements-cyclables/records",
        params={
            "select": "arrondissement, sum(st_length_shape) as longueur_m",
            "group_by": "arrondissement",
            "limit": 25,
        },
        timeout=15,
    )
    infra = r.json().get("results", [])
    con.execute("CREATE TABLE infra (arrondissement INTEGER, longueur_km DOUBLE)")
    con.executemany(
        "INSERT INTO infra VALUES (?, ?)",
        [(int(d["arrondissement"]), round(d["longueur_m"] / 1000, 1)) for d in infra],
    )

    con.execute("DROP TABLE IF EXISTS pg.public.cyclable_paris")
    con.execute(
        """CREATE TABLE pg.public.cyclable_paris AS
        SELECT '751' || lpad(i.arrondissement::text, 2, '0') as code_commune,
               g.nom as nom_commune,
               i.arrondissement, i.longueur_km,
               round((i.longueur_km / a.area_km2)::numeric, 1) as km_par_km2
        FROM infra i
        JOIN areas a ON a.code = '751' || lpad(i.arrondissement::text, 2, '0')
        JOIN geo g ON g.code = a.code
        ORDER BY i.arrondissement"""
    )
    print(f"  cyclable_paris — {len(infra)} arrondissements")

    # Top bike counters with coordinates (latest full month)
    # 1) Get counter locations
    r_loc = httpx.get(
        "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
        "comptage-velo-compteurs/records",
        params={"limit": 100},
        timeout=15,
    )
    loc_data = {
        d["id_compteur"]: d.get("coordinates", {})
        for d in r_loc.json().get("results", [])
    }

    # 2) Get monthly traffic
    r2 = httpx.get(
        "https://opendata.paris.fr/api/explore/v2.1/catalog/datasets/"
        "comptage-velo-donnees-compteurs/records",
        params={
            "select": "id_compteur, nom_compteur, sum(sum_counts) as total_passages",
            "where": 'date >= "2026-03-01" and date < "2026-04-01"',
            "group_by": "id_compteur, nom_compteur",
            "order_by": "total_passages desc",
            "limit": 30,
        },
        timeout=15,
    )
    counters = r2.json().get("results", [])

    con.execute(
        "CREATE TABLE counters (nom VARCHAR, lat DOUBLE, lon DOUBLE, total_passages INTEGER)"
    )
    con.executemany(
        "INSERT INTO counters VALUES (?, ?, ?, ?)",
        [
            (
                d["nom_compteur"],
                loc_data.get(d["id_compteur"], {}).get("lat"),
                loc_data.get(d["id_compteur"], {}).get("lon"),
                d["total_passages"],
            )
            for d in counters
        ],
    )

    con.execute("DROP TABLE IF EXISTS pg.public.comptage_velo_paris")
    con.execute(
        """CREATE TABLE pg.public.comptage_velo_paris AS
        SELECT * FROM counters WHERE lat IS NOT NULL ORDER BY total_passages DESC"""
    )
    rows = con.execute("SELECT count(*) FROM pg.public.comptage_velo_paris").fetchone()[
        0
    ]
    print(f"  comptage_velo_paris — {rows} compteurs géolocalisés")

    con.execute("DETACH pg")
    con.close()


def export_metro_to_postgres() -> None:
    """Download metro+RER station data from IDFM and aggregate by commune."""
    # Fetch all metro + RER stops (paginated)
    stops: list[dict] = []
    for mode in ("Metro", "RER"):
        offset = 0
        while True:
            r = httpx.get(
                "https://data.iledefrance-mobilites.fr/api/explore/v2.1/catalog/datasets/"
                "arrets-lignes/records",
                params={
                    "limit": 100,
                    "offset": offset,
                    "where": f'mode = "{mode}"',
                    "select": "stop_name, stop_lat, stop_lon, mode, shortname",
                },
                timeout=15,
            )
            batch = r.json().get("results", [])
            stops.extend(batch)
            if len(batch) < 100:
                break
            offset += 100

    # Deduplicate: unique physical stations with their line count
    stations: dict[str, dict] = {}
    for s in stops:
        name = s["stop_name"]
        if name not in stations:
            stations[name] = {
                "lat": float(s["stop_lat"]),
                "lon": float(s["stop_lon"]),
                "lines": set(),
                "modes": set(),
            }
        stations[name]["lines"].add(s["shortname"])
        stations[name]["modes"].add(s["mode"])
    print(
        f"  {len(stops)} arrêts IDFM → {len(stations)} stations physiques (métro+RER)"
    )

    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial;")
    con.execute("INSTALL postgres; LOAD postgres;")
    con.execute(f"ATTACH '{PG_CONN}' AS pg (TYPE POSTGRES)")

    con.execute(
        "CREATE TABLE stops (name VARCHAR, lat DOUBLE, lon DOUBLE, nb_lines INTEGER, modes VARCHAR)"
    )
    con.executemany(
        "INSERT INTO stops VALUES (?, ?, ?, ?, ?)",
        [
            (n, d["lat"], d["lon"], len(d["lines"]), "+".join(sorted(d["modes"])))
            for n, d in stations.items()
        ],
    )

    geojson_path = str(GEOJSON_DIR / "idf_communes.geojson")
    con.execute(f"CREATE TABLE geo AS SELECT * FROM ST_Read('{geojson_path}')")

    con.execute("DROP TABLE IF EXISTS pg.public.metro_stations")
    con.execute(
        """
        CREATE TABLE pg.public.metro_stations AS
        WITH areas AS (
            SELECT code, nom, ST_Area(geom) * 111.12 * 111.12 * 0.6583 as area_km2
            FROM geo
        ),
        counts AS (
            SELECT g.code as code_commune, g.nom as nom_commune,
                   count(*)::integer as nb_stations,
                   sum(s.nb_lines)::integer as nb_lignes_accessibles
            FROM stops s
            JOIN geo g ON ST_Contains(g.geom, ST_Point(s.lon, s.lat))
            GROUP BY g.code, g.nom
        )
        SELECT c.code_commune, c.nom_commune,
               c.nb_stations, c.nb_lignes_accessibles,
               round(a.area_km2::numeric, 2) as superficie_km2,
               round((c.nb_stations / a.area_km2)::numeric, 1) as stations_par_km2
        FROM counts c JOIN areas a ON c.code_commune = a.code
        ORDER BY c.code_commune
    """
    )
    rows = con.execute("SELECT count(*) FROM pg.public.metro_stations").fetchone()[0]
    print(f"  metro_stations — {rows} communes desservies")

    con.execute("DETACH pg")
    con.close()


def export_diplomes_to_postgres() -> None:
    """Download INSEE diploma data and compute education metrics by commune."""
    import io
    import zipfile

    r = httpx.get(
        "https://www.insee.fr/fr/statistiques/fichier/8202319/"
        "base-cc-diplomes-formation-2021_csv.zip",
        timeout=60,
        follow_redirects=True,
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
    con.execute(
        f"""
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
    """
    )
    rows = con.execute("SELECT count(*) FROM pg.public.diplomes_communes").fetchone()[0]
    print(f"  diplomes_communes — {rows:,} communes")

    con.execute("DETACH pg")
    con.close()


def generate_geojson() -> None:
    """Download and generate zone-specific GeoJSON files for maps."""
    GEOJSON_DIR.mkdir(parents=True, exist_ok=True)

    required_files = [
        "idf_communes.geojson",
        "paris_arrondissements.geojson",
        "petite_couronne.geojson",
        "petite_couronne_plus_paris.geojson",
        "grande_couronne.geojson",
    ]
    if all((GEOJSON_DIR / filename).exists() for filename in required_files):
        print("  GeoJSON déjà présent")
        return

    def write_geo(name, feats):
        path = GEOJSON_DIR / f"{name}.geojson"
        with open(path, "w") as f:
            json.dump(
                {"type": "FeatureCollection", "features": feats},
                f,
                separators=(",", ":"),
            )
        print(
            f"  {name}.geojson — {len(feats)} features ({path.stat().st_size // 1024}KB)"
        )

    def write_zone_files(features):
        write_geo("idf_communes", features)
        write_geo(
            "paris_arrondissements",
            [f for f in features if f["properties"]["code"].startswith("751")],
        )
        write_geo(
            "petite_couronne",
            [f for f in features if f["properties"]["code"][:2] in ("92", "93", "94")],
        )
        write_geo(
            "petite_couronne_plus_paris",
            [
                f
                for f in features
                if f["properties"]["code"].startswith("751")
                or f["properties"]["code"][:2] in ("92", "93", "94")
            ],
        )
        write_geo(
            "grande_couronne",
            [
                f
                for f in features
                if f["properties"]["code"][:2] in ("77", "78", "91", "95")
            ],
        )

    idf_path = GEOJSON_DIR / "idf_communes.geojson"
    if idf_path.exists():
        features = json.loads(idf_path.read_text(encoding="utf-8"))["features"]
        write_zone_files(features)
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
        feat["geometry"]["coordinates"] = simplify_coords(
            feat["geometry"]["coordinates"]
        )
        props = feat["properties"]
        feat["properties"] = {"code": props["code"], "nom": props["nom"]}

    write_zone_files(features)


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
        r = client.post(
            "/api/session",
            json={
                "username": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
            },
        )
        r.raise_for_status()
        return r.json()["id"]

    print("  Configuration initiale de Metabase…")
    r = client.post(
        "/api/setup",
        json={
            "token": props["setup-token"],
            "user": {
                "email": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
                "first_name": "Admin",
                "last_name": "Panorama IDF",
                "site_name": "Panorama Ile-de-France",
            },
            "prefs": {
                "site_name": "Panorama Ile-de-France",
                "site_locale": "fr",
                "allow_tracking": False,
            },
        },
    )
    if r.status_code == 403:
        print("  Setup déjà fait, connexion…")
        r = client.post(
            "/api/session",
            json={
                "username": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
            },
        )
        r.raise_for_status()
        return r.json()["id"]
    r.raise_for_status()
    return r.json()["id"]


def add_postgres_database(client: httpx.Client) -> int:
    """Add PostgreSQL database connection to Metabase (or rename legacy entry)."""
    target_name = "Panorama Ile-de-France"
    legacy_names = {"France Aujourd'hui"}  # migration douce des anciens setups

    r = client.get("/api/database")
    dbs = r.json().get("data", [])
    for db in dbs:
        name = db.get("name")
        if name == target_name:
            print(f"  Base déjà configurée (id={db['id']})")
            client.post(f"/api/database/{db['id']}/sync_schema")
            return db["id"]
        if name in legacy_names:
            print(f"  Renommage base legacy '{name}' → '{target_name}' (id={db['id']})")
            client.put(f"/api/database/{db['id']}", json={"name": target_name})
            client.post(f"/api/database/{db['id']}/sync_schema")
            return db["id"]

    print("  Ajout de la base PostgreSQL…")
    # En prod on depose un vrai mdp via POSTGRES_PASSWORD ; en dev la valeur defaut est "metabase".
    r = client.post(
        "/api/database",
        json={
            "name": target_name,
            "engine": "postgres",
            "details": {
                "host": "postgres",
                "port": 5432,
                "dbname": _pg_db,
                "user": _pg_user,
                "password": _pg_password,
            },
        },
    )
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
        ("petite_couronne_plus_paris", "Petite couronne + Paris"),
        ("grande_couronne", "Communes Grande couronne"),
    ]:
        filename = f"{key.replace('paris_arr', 'paris_arrondissements')}.geojson"
        maps[key] = {
            "name": name,
            "url": f"{GEOJSON_BASE_URL}/{filename}",
            "region_key": "code",
            "region_name": "nom",
        }
    client.put("/api/setting/custom-geojson", json={"value": maps})
    print("  5 cartes GeoJSON enregistrées")


# ── Card helper ─────────────────────────────────────────────────────


def make_card(
    client: httpx.Client,
    db_id: int,
    name: str,
    display: str,
    query: str,
    desc: str = "",
    viz: dict | None = None,
    template_tags: dict | None = None,
) -> int:
    """Create a Metabase card (question)."""
    native_query = {"query": query}
    if template_tags:
        native_query["template-tags"] = template_tags
    r = client.post(
        "/api/card",
        json={
            "name": name,
            "description": desc or None,
            "dataset_query": {
                "type": "native",
                "native": native_query,
                "database": db_id,
            },
            "display": display,
            "visualization_settings": viz or {},
        },
    )
    r.raise_for_status()
    cid = r.json()["id"]
    print(f"  {cid}: {name}")
    return cid


# Single alternative color for right-side maps (light green)
CLR_ALT = ["#E8F5E9", "#C8E6C9", "#A5D6A7", "#81C784", "#66BB6A"]


def map_viz(region: str, metric: str, colors: list[str] | None = None) -> dict:
    viz = {
        "map.type": "region",
        "map.region": region,
        "map.dimension": "code_commune",
        "map.metric": metric,
    }
    if colors:
        viz["map.colors"] = colors
    return viz


# ── Dashboard creation ──────────────────────────────────────────────

Y = str(LATEST_YEAR)
PC_INCLUDE_PARIS_TAG = {
    "include_paris": {
        "id": "include_paris",
        "name": "include_paris",
        "display-name": "Inclure Paris",
        "type": "boolean",
    }
}
PC_INCLUDE_PARIS_PARAMETER = {
    "id": "pc_include_paris",
    "name": "Petite couronne : inclure Paris",
    "slug": "pc_include_paris",
    "type": "boolean/=",
}

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
    return {
        "id": id_,
        "card_id": None,
        "dashboard_tab_id": tab,
        "row": row,
        "col": 0,
        "size_x": 24,
        "size_y": 2,
        "visualization_settings": {
            "virtual_card": {"name": None, "display": "text", "archived": False},
            "text": f"## {text}",
            "dashcard.background": False,
            "text.align_vertical": "bottom",
            "text.align_horizontal": "center",
        },
    }


def _text(id_: int, tab: int, row: int, col: int, sx: int, sy: int, text: str) -> dict:
    """Dashboard text/legend card (left-aligned, transparent background)."""
    return {
        "id": id_,
        "card_id": None,
        "dashboard_tab_id": tab,
        "row": row,
        "col": col,
        "size_x": sx,
        "size_y": sy,
        "visualization_settings": {
            "virtual_card": {"name": None, "display": "text", "archived": False},
            "text": text,
            "dashcard.background": False,
            "text.align_vertical": "top",
            "text.align_horizontal": "left",
        },
    }


DELIN_LEGEND = """\
**Détail des catégories (indicateurs SSMSI) :**

- **Vols** : avec/sans violence, avec armes, de/dans véhicules, accessoires
- **Violences** : physiques (hors et intra-familiales), sexuelles
- **Stupéfiants** : trafic et usage (incl. AFD)
- **Atteintes aux biens** : cambriolages de logement, destructions et dégradations volontaires
- **Escroqueries** : escroqueries et fraudes aux moyens de paiement"""

INTRO_TEXT = """\
**Sources :** Immobilier DVF 2020-2025 (Cerema) · Revenus, démographie, éducation RP 2021 (INSEE) · Loyers 2025 (ANIL) · Délinquance 2016-2024 (SSMSI) · Mobilité (Vélib, IDFM, Paris OpenData)"""

# Remplacer par l'URL de votre formulaire Tally.so (https://tally.so)
FEEDBACK_URL = "https://tally.so/r/7RV51A"

FEEDBACK_TEXT = f"""\
### Une suggestion ou une idée d'analyse ?

Ce dashboard évolue avec vos retours. Partagez vos idées, commentaires
ou suggestions d'analyses complémentaires :

**[→ Ouvrir le formulaire de suggestion]({FEEDBACK_URL})**"""


def _pc_scope(
    zone_field: str | None = None,
    dept_field: str | None = None,
    code_field: str | None = None,
) -> str:
    """Return the Petite couronne perimeter, with optional Paris inclusion."""
    if zone_field and dept_field:
        return (
            f"({zone_field} = 'Petite couronne'"
            f" [[ OR ({{{{include_paris}}}} AND {dept_field} = '75') ]])"
        )
    if dept_field:
        return (
            f"({dept_field} IN ('92', '93', '94')"
            f" [[ OR ({{{{include_paris}}}} AND {dept_field} = '75') ]])"
        )
    if code_field:
        return (
            f"(left({code_field}, 2) IN ('92', '93', '94')"
            f" [[ OR ({{{{include_paris}}}} AND {code_field} LIKE '751%') ]])"
        )
    raise ValueError("At least one field must be provided")


def create_tabbed_dashboard(client: httpx.Client, db_id: int) -> int:
    """Create (or replace) the full 3-tab dashboard with all cards."""

    # ── Find or create dashboard (keep exactly one) ──
    # On utilise /api/dashboard/ plutot que /api/search qui depend d'un
    # index pas toujours initialise sur une instance Metabase fraiche.
    dash_id = None
    r = client.get("/api/dashboard/")
    items = r.json() if r.status_code == 200 else []
    if not isinstance(items, list):
        items = []
    for item in items:
        if not isinstance(item, dict) or item.get("archived"):
            continue
        if dash_id is None and item.get("name") == "Panorama Ile-de-France":
            dash_id = item["id"]
            print(f"  Dashboard existant (id={dash_id}), mise a jour…")
        else:
            client.delete(f"/api/dashboard/{item['id']}")
            print(f"  Ancien dashboard {item['id']} supprime")

    # ── IDF tab cards ──
    c_idf_map_prix = make_card(
        client,
        db_id,
        "Prix au m2 — Ile-de-France",
        "map",
        f"SELECT code_commune, nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes, zone_idf\nFROM mart_immo__accessibilite_commune WHERE annee = {Y}",
        f"Prix médian au m² par commune. Source : DVF {Y} (Cerema).",
        map_viz("idf_communes", "prix_m2"),
    )

    c_idf_map_age = make_card(
        client,
        db_id,
        "Part des 25-39 ans — Ile-de-France",
        "map",
        f"SELECT code_commune, nom_commune, round((part_25_39 * 100)::numeric, 1) as pct_25_39, zone_idf\nFROM mart_immo__accessibilite_commune WHERE annee = {Y}",
        "Part des 25-39 ans dans la population. Source : RP 2021 (INSEE).",
        map_viz("idf_communes", "pct_25_39"),
    )

    c_idf_synthese = make_card(
        client,
        db_id,
        "Synthese par zone",
        "table",
        """SELECT zone_idf, annee::text as annee, nb_communes, nb_ventes_total,
       round(prix_m2_median_pondere::numeric) as prix_m2,
       round(niveau_vie_median_pondere::numeric) as niveau_vie,
       round(ratio_achat_revenu_pondere::numeric, 1) as ratio_achat,
       round((part_25_39_ponderee * 100)::numeric, 1) as pct_25_39
FROM mart_immo__synthese_zone ORDER BY annee DESC, zone_idf""",
        "Agrégats pondérés par zone. Prix DVF 2020-2025, revenus Filosofi 2021, démographie RP 2021.",
    )

    c_idf_prix = make_card(
        client,
        db_id,
        "Evolution prix au m2 par zone",
        "line",
        """SELECT annee::text as annee, zone_idf, round(prix_m2_median_pondere::numeric) as prix_m2
FROM mart_immo__synthese_zone ORDER BY annee, zone_idf""",
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["annee", "zone_idf"],
            "graph.metrics": ["prix_m2"],
            "graph.y_axis.title_text": "EUR / m2",
        },
    )

    c_idf_volume = make_card(
        client,
        db_id,
        "Volume de ventes par zone",
        "bar",
        """SELECT annee::text as annee, zone_idf, nb_ventes_total
FROM mart_immo__synthese_zone ORDER BY annee, zone_idf""",
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["annee", "zone_idf"],
            "graph.metrics": ["nb_ventes_total"],
            "graph.y_axis.title_text": "Ventes",
            "stackable.stack_type": "stacked",
        },
    )

    c_idf_scatter = make_card(
        client,
        db_id,
        f"Jeunes adultes vs prix au m2 ({Y})",
        "scatter",
        f"""SELECT nom_commune, zone_idf, prix_m2_median,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND part_25_39 IS NOT NULL
ORDER BY prix_m2_median DESC""",
        f"Chaque point = une commune. Sources : prix DVF {Y} (Cerema), démographie RP 2021 (INSEE).",
        viz={
            "graph.dimensions": ["prix_m2_median"],
            "graph.metrics": ["pct_25_39"],
            "graph.x_axis.title_text": "Prix m2 (EUR)",
            "graph.y_axis.title_text": "% 25-39 ans",
        },
    )

    c_idf_ratio = make_card(
        client,
        db_id,
        "Ratio prix / revenu par zone",
        "line",
        """SELECT annee::text as annee, zone_idf,
       round(ratio_achat_revenu_pondere::numeric, 1) as ratio_achat
FROM mart_immo__synthese_zone ORDER BY annee, zone_idf""",
        "Années de revenu (Filosofi 2021) pour un achat médian (DVF 2020-2025).",
        viz={
            "graph.dimensions": ["annee", "zone_idf"],
            "graph.metrics": ["ratio_achat"],
            "graph.y_axis.title_text": "Annees de revenu",
        },
    )

    c_idf_surface = make_card(
        client,
        db_id,
        "Surface mediane par zone",
        "line",
        """SELECT annee::text as annee, zone_idf,
       round(sum(surface_mediane * nb_ventes) / nullif(sum(nb_ventes), 0)) as surface_m2
FROM mart_immo__accessibilite_commune
GROUP BY annee, zone_idf ORDER BY annee, zone_idf""",
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["annee", "zone_idf"],
            "graph.metrics": ["surface_m2"],
            "graph.y_axis.title_text": "Surface (m2)",
        },
    )

    c_idf_map_loyer = make_card(
        client,
        db_id,
        "Loyer au m2 — Ile-de-France",
        "map",
        f"SELECT code_commune, nom_commune, round(loyer_m2_median::numeric, 1) as loyer_m2, zone_idf\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND loyer_m2_median IS NOT NULL",
        "Loyer prédit au m² (appartements). Source : Carte des loyers 2025 (ANIL).",
        map_viz("idf_communes", "loyer_m2"),
    )

    c_idf_loyer_vs_achat = make_card(
        client,
        db_id,
        "Loyer vs prix d'achat au m2 par zone",
        "bar",
        f"""SELECT zone_idf,
       round((sum(loyer_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0))::numeric, 1) as loyer_m2_mois,
       round((sum(prix_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0) / 12)::numeric, 0) as mensualite_achat_m2
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND loyer_m2_median IS NOT NULL
GROUP BY zone_idf ORDER BY zone_idf""",
        f"Loyer mensuel ANIL 2025 vs mensualité d'achat (1/12 du prix DVF {Y}).",
        viz={
            "graph.dimensions": ["zone_idf"],
            "graph.metrics": ["loyer_m2_mois", "mensualite_achat_m2"],
            "graph.y_axis.title_text": "EUR / m2",
        },
    )

    c_idf_map_delin = make_card(
        client,
        db_id,
        "Delinquance — Ile-de-France",
        "map",
        f"""SELECT code_commune, nom_commune,
       round(taux_delinquance_pour_mille::numeric, 1) as taux_delin, zone_idf
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND taux_delinquance_pour_mille IS NOT NULL""",
        "Faits pour 1000 habitants, dernière année disponible. Source : SSMSI / Min. Intérieur (2016-2024).",
        map_viz("idf_communes", "taux_delin"),
    )

    c_idf_evol_delin = make_card(
        client,
        db_id,
        "Evolution delinquance par zone",
        "line",
        """SELECT annee::text as annee, zone_idf,
       round((sum(nb_faits_delinquance::numeric) / nullif(sum(population_2021), 0) * 1000)::numeric, 1) as taux_delin
FROM mart_immo__accessibilite_commune
WHERE taux_delinquance_pour_mille IS NOT NULL
GROUP BY annee, zone_idf ORDER BY annee, zone_idf""",
        "Source : SSMSI / Min. Intérieur (2016-2024).",
        viz={
            "graph.dimensions": ["annee", "zone_idf"],
            "graph.metrics": ["taux_delin"],
            "graph.y_axis.title_text": "Faits / 1000 hab.",
        },
    )

    c_idf_delin_type = make_card(
        client,
        db_id,
        "Delinquance par categorie — Ile-de-France",
        "bar",
        f"""SELECT {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux_pour_mille
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE d.annee = 2024 AND a.zone_idf IS NOT NULL
GROUP BY categorie ORDER BY taux_pour_mille DESC""",
        "Source : SSMSI / Min. Intérieur (année 2024).",
        viz={
            "graph.dimensions": ["categorie"],
            "graph.metrics": ["taux_pour_mille"],
            "graph.y_axis.title_text": "Faits / 1000 hab.",
        },
    )

    c_idf_evol_delits = make_card(
        client,
        db_id,
        "Evolution des delits par categorie — IDF",
        "line",
        f"""SELECT d.annee::text as annee, {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE a.zone_idf IS NOT NULL
GROUP BY d.annee, categorie ORDER BY d.annee, categorie""",
        "Source : SSMSI / Min. Intérieur (2016-2024).",
        viz={
            "graph.dimensions": ["annee", "categorie"],
            "graph.metrics": ["taux"],
            "graph.y_axis.title_text": "Faits / 1000 hab.",
        },
    )

    c_idf_map_60plus = make_card(
        client,
        db_id,
        "Part des 60+ ans — Ile-de-France",
        "map",
        f"""SELECT code_commune, nom_commune,
       round((part_60_plus * 100)::numeric, 1) as pct_60_plus, zone_idf
FROM mart_immo__accessibilite_commune WHERE annee = {Y} AND part_60_plus IS NOT NULL""",
        "Part des 60 ans et plus dans la population. Source : RP 2021 (INSEE).",
        map_viz("idf_communes", "pct_60_plus"),
    )

    c_idf_map_diplome = make_card(
        client,
        db_id,
        "Part des diplômés du supérieur — Ile-de-France",
        "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_etudes_sup, a.zone_idf
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND d.part_etudes_sup IS NOT NULL""",
        "Part de la population 15+ ayant un diplôme bac+2 et plus. Source : RP 2021 (INSEE).",
        map_viz("idf_communes", "part_etudes_sup"),
    )

    c_idf_map_sans_diplome = make_card(
        client,
        db_id,
        "Part sans diplôme — Ile-de-France",
        "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_sans_diplome, a.zone_idf
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND d.part_sans_diplome IS NOT NULL""",
        "Part de la population 15+ sans diplôme ou avec CEP. Source : RP 2021 (INSEE).",
        map_viz("idf_communes", "part_sans_diplome"),
    )

    # ── Paris tab cards ──
    c_paris_map_prix = make_card(
        client,
        db_id,
        "Prix au m2 — Paris",
        "map",
        f"SELECT code_commune, nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND code_commune LIKE '751%'",
        f"Prix médian au m² par arrondissement. Source : DVF {Y} (Cerema).",
        map_viz("paris_arr", "prix_m2"),
    )

    c_paris_map_age = make_card(
        client,
        db_id,
        "Part des 25-39 ans — Paris",
        "map",
        f"SELECT code_commune, nom_commune, round((part_25_39 * 100)::numeric, 1) as pct_25_39\nFROM mart_immo__accessibilite_commune WHERE annee = {Y} AND code_commune LIKE '751%'",
        "Part des 25-39 ans dans la population. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "pct_25_39"),
    )

    c_paris_evol = make_card(
        client,
        db_id,
        "Evolution prix au m2 par arrondissement",
        "line",
        """SELECT annee::text as annee, nom_commune, round(prix_m2_median::numeric) as prix_m2
FROM mart_immo__evolution_prix WHERE code_commune LIKE '751%'
ORDER BY annee, nom_commune""",
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["annee", "nom_commune"],
            "graph.metrics": ["prix_m2"],
            "graph.y_axis.title_text": "EUR / m2",
        },
    )

    c_paris_table = make_card(
        client,
        db_id,
        f"Detail par arrondissement ({Y})",
        "table",
        f"""SELECT nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(niveau_vie_median::numeric) as revenu, round(surface_mediane::numeric, 1) as surface_m2
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY prix_m2_median DESC""",
        f"Sources : prix et surface DVF {Y} (Cerema), démographie RP 2021, revenus Filosofi 2021 (INSEE).",
    )

    c_paris_surface = make_card(
        client,
        db_id,
        f"Surface mediane par arrondissement ({Y})",
        "bar",
        f"""SELECT nom_commune, round(surface_mediane::numeric, 1) as surface_m2, nb_ventes
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY surface_mediane DESC""",
        f"Source : DVF {Y} (Cerema).",
        viz={
            "graph.dimensions": ["nom_commune"],
            "graph.metrics": ["surface_m2"],
            "graph.y_axis.title_text": "Surface (m2)",
        },
    )

    c_paris_map_loyer_studio = make_card(
        client,
        db_id,
        "Loyer estime studio — Paris",
        "map",
        f"""SELECT a.code_commune, a.nom_commune,
       round((a.loyer_m2_median * p.surface_mediane)::numeric) as loyer_mensuel
FROM mart_immo__accessibilite_commune a
JOIN prix_paris_par_pieces p
    ON a.code_commune = p.code_commune AND a.annee = p.annee
WHERE a.annee = {Y} AND p.nb_pieces = 1 AND a.loyer_m2_median IS NOT NULL""",
        f"Loyer m² ANIL 2025 × surface médiane studio DVF {Y}.",
        map_viz("paris_arr", "loyer_mensuel"),
    )

    c_paris_map_loyer_2p = make_card(
        client,
        db_id,
        "Loyer estimé 2 pièces — Paris",
        "map",
        f"""SELECT a.code_commune, a.nom_commune,
       round((a.loyer_m2_median * p.surface_mediane)::numeric) as loyer_mensuel
FROM mart_immo__accessibilite_commune a
JOIN prix_paris_par_pieces p
    ON a.code_commune = p.code_commune AND a.annee = p.annee
WHERE a.annee = {Y} AND p.nb_pieces = 2 AND a.loyer_m2_median IS NOT NULL""",
        f"Loyer m² ANIL 2025 × surface médiane 2P DVF {Y}.",
        map_viz("paris_arr", "loyer_mensuel"),
    )

    c_paris_map_delin = make_card(
        client,
        db_id,
        "Delinquance — Paris",
        "map",
        f"""SELECT code_commune, nom_commune,
       round(taux_delinquance_pour_mille::numeric, 1) as taux_delin
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND code_commune LIKE '751%' AND taux_delinquance_pour_mille IS NOT NULL""",
        "Faits pour 1000 habitants, dernière année disponible. Source : SSMSI / Min. Intérieur (2016-2024).",
        map_viz("paris_arr", "taux_delin"),
    )

    c_paris_delin_cat = make_card(
        client,
        db_id,
        "Delinquance par categorie et arrondissement",
        "bar",
        f"""SELECT cast(right(d.code_commune, 2) as integer) as arrdt,
       {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux_pour_mille
FROM stg_delinquance_detail d
WHERE d.annee = 2024 AND d.code_commune LIKE '751%'
GROUP BY arrdt, categorie ORDER BY arrdt, taux_pour_mille DESC""",
        "Source : SSMSI / Min. Intérieur (année 2024).",
        viz={
            "graph.dimensions": ["arrdt", "categorie"],
            "graph.metrics": ["taux_pour_mille"],
            "graph.y_axis.title_text": "Faits / 1000 hab.",
            "stackable.stack_type": "stacked",
        },
    )

    c_paris_map_velib = make_card(
        client,
        db_id,
        "Densité Vélib — Paris (stations/km²)",
        "map",
        """SELECT code_commune, nom_commune, stations_par_km2
FROM velib_stations WHERE code_commune LIKE '751%'""",
        "Stations Vélib par km². Source : Vélib Métropole (snapshot temps réel).",
        map_viz("paris_arr", "stations_par_km2"),
    )

    c_paris_map_cyclable = make_card(
        client,
        db_id,
        "Densité pistes cyclables — Paris (km/km²)",
        "map",
        """SELECT code_commune, nom_commune, km_par_km2
FROM cyclable_paris""",
        "Linéaire d'aménagements cyclables par km². Source : Paris OpenData (snapshot).",
        map_viz("paris_arr", "km_par_km2"),
    )

    c_paris_map_metro = make_card(
        client,
        db_id,
        "Densité métro + RER — Paris (stations/km²)",
        "map",
        """SELECT code_commune, nom_commune, stations_par_km2, nb_lignes_accessibles
FROM metro_stations WHERE code_commune LIKE '751%'""",
        "Stations métro et RER par km² (uniques par nom). Source : IDFM Open Data.",
        map_viz("paris_arr", "stations_par_km2"),
    )

    c_paris_map_60plus = make_card(
        client,
        db_id,
        "Part des 60+ ans — Paris",
        "map",
        f"""SELECT code_commune, nom_commune,
       round((part_60_plus * 100)::numeric, 1) as pct_60_plus
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' AND part_60_plus IS NOT NULL""",
        "Part des 60 ans et plus dans la population. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "pct_60_plus"),
    )

    c_paris_map_diplome = make_card(
        client,
        db_id,
        "Part des diplômés du supérieur — Paris",
        "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_etudes_sup
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND a.code_commune LIKE '751%'""",
        "Part de la population 15+ ayant un diplôme bac+2 et plus. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "part_etudes_sup"),
    )

    c_paris_map_sans_diplome = make_card(
        client,
        db_id,
        "Part sans diplôme — Paris",
        "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_sans_diplome
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND a.code_commune LIKE '751%'""",
        "Part de la population 15+ sans diplôme ou avec CEP. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "part_sans_diplome"),
    )

    # ── Petite couronne tab cards ──
    c_pc_map_prix = make_card(
        client,
        db_id,
        "Prix au m2 — Petite couronne",
        "map",
        f"""SELECT code_commune, nom_commune, round(prix_m2_median::numeric) as prix_m2, nb_ventes
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}""",
        f"Prix médian au m² par commune. Source : DVF {Y} (Cerema).",
        map_viz("petite_couronne_plus_paris", "prix_m2"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_age = make_card(
        client,
        db_id,
        "Part des 25-39 ans — Petite couronne",
        "map",
        f"""SELECT code_commune, nom_commune, round((part_25_39 * 100)::numeric, 1) as pct_25_39
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}""",
        "Part des 25-39 ans dans la population. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "pct_25_39"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_evol = make_card(
        client,
        db_id,
        "Evolution prix m2 — top 10 communes",
        "line",
        f"""SELECT e.annee::text as annee, e.nom_commune, round(e.prix_m2_median::numeric) as prix_m2
FROM mart_immo__evolution_prix e
WHERE {_pc_scope(dept_field='e.code_departement')}
  AND e.code_commune IN (
    SELECT code_commune FROM mart_immo__accessibilite_commune
    WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}
    ORDER BY nb_ventes DESC LIMIT 10)
ORDER BY annee, nom_commune""",
        "Top 10 communes par volume de ventes. Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["annee", "nom_commune"],
            "graph.metrics": ["prix_m2"],
            "graph.y_axis.title_text": "EUR / m2",
        },
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_table = make_card(
        client,
        db_id,
        f"Top 20 communes ({Y})",
        "table",
        f"""SELECT nom_commune, code_departement as dept,
       round(prix_m2_median::numeric) as prix_m2, nb_ventes,
       round((part_25_39 * 100)::numeric, 1) as pct_25_39,
       round(niveau_vie_median::numeric) as revenu,
       round(ratio_achat_revenu_annuel::numeric, 1) as ratio_achat
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}
ORDER BY nb_ventes DESC LIMIT 20""",
        f"Classées par volume de ventes. Sources : DVF {Y}, RP 2021, Filosofi 2021.",
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_loyer = make_card(
        client,
        db_id,
        "Loyer au m2 — Petite couronne",
        "map",
        f"""SELECT code_commune, nom_commune, round(loyer_m2_median::numeric, 1) as loyer_m2
FROM mart_immo__accessibilite_commune
WHERE annee = {Y}
  AND {_pc_scope('zone_idf', 'code_departement')}
  AND loyer_m2_median IS NOT NULL""",
        "Loyer prédit au m² (appartements). Source : Carte des loyers 2025 (ANIL).",
        map_viz("petite_couronne_plus_paris", "loyer_m2"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_delin = make_card(
        client,
        db_id,
        "Delinquance — Petite couronne",
        "map",
        f"""SELECT code_commune, nom_commune,
       round(taux_delinquance_pour_mille::numeric, 1) as taux_delin
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND {_pc_scope('zone_idf', 'code_departement')}
  AND taux_delinquance_pour_mille IS NOT NULL""",
        "Faits pour 1000 habitants, dernière année disponible. Source : SSMSI / Min. Intérieur (2016-2024).",
        map_viz("petite_couronne_plus_paris", "taux_delin"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_delin_cat = make_card(
        client,
        db_id,
        "Delinquance par categorie — Petite couronne",
        "bar",
        f"""SELECT {DELIN_CATEGORIES} as categorie,
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux_pour_mille
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE d.annee = 2024 AND {_pc_scope('a.zone_idf', 'a.code_departement')}
GROUP BY categorie ORDER BY taux_pour_mille DESC""",
        "Source : SSMSI / Min. Intérieur (année 2024).",
        viz={
            "graph.dimensions": ["categorie"],
            "graph.metrics": ["taux_pour_mille"],
            "graph.y_axis.title_text": "Faits / 1000 hab.",
        },
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_velib = make_card(
        client,
        db_id,
        "Densité Vélib — Petite couronne (stations/km²)",
        "map",
        f"""SELECT code_commune, nom_commune, stations_par_km2
FROM velib_stations
WHERE {_pc_scope(code_field='code_commune')}""",
        "Stations Vélib par km². Source : Vélib Métropole (snapshot temps réel).",
        map_viz("petite_couronne_plus_paris", "stations_par_km2"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_metro = make_card(
        client,
        db_id,
        "Densité métro + RER — Petite couronne (stations/km²)",
        "map",
        f"""SELECT code_commune, nom_commune, stations_par_km2
FROM metro_stations
WHERE {_pc_scope(code_field='code_commune')}""",
        "Stations métro et RER par km². Source : IDFM Open Data.",
        map_viz("petite_couronne_plus_paris", "stations_par_km2"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_60plus = make_card(
        client,
        db_id,
        "Part des 60+ ans — Petite couronne",
        "map",
        f"""SELECT code_commune, nom_commune,
       round((part_60_plus * 100)::numeric, 1) as pct_60_plus
FROM mart_immo__accessibilite_commune
WHERE annee = {Y}
  AND {_pc_scope('zone_idf', 'code_departement')}
  AND part_60_plus IS NOT NULL""",
        "Part des 60 ans et plus dans la population. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "pct_60_plus"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_diplome = make_card(
        client,
        db_id,
        "Part des diplômés du supérieur — Petite couronne",
        "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_etudes_sup
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND {_pc_scope('a.zone_idf', 'a.code_departement')}""",
        "Part de la population 15+ ayant un diplôme bac+2 et plus. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "part_etudes_sup"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_sans_diplome = make_card(
        client,
        db_id,
        "Part sans diplôme — Petite couronne",
        "map",
        f"""SELECT a.code_commune, a.nom_commune, d.part_sans_diplome
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND {_pc_scope('a.zone_idf', 'a.code_departement')}""",
        "Part de la population 15+ sans diplôme ou avec CEP. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "part_sans_diplome"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    # ── Create dashboard if needed ──
    if dash_id is None:
        r = client.post(
            "/api/dashboard",
            json={
                "name": "Panorama Ile-de-France",
                "description": "Immobilier, revenus, loyers, demographie et securite par commune (2020-2025).",
            },
        )
        r.raise_for_status()
        dash_id = r.json()["id"]
        print(f"\n  Dashboard cree (id={dash_id})")
    print()

    # ── Layout with section headings ──
    T1, T2, T3 = -1, -2, -3  # tab IDs
    n = 0  # auto-increment dashcard IDs

    def _card(card_id, tab, row, col, sx, sy, parameter_mappings=None):
        nonlocal n
        n -= 1
        dashcard = {
            "id": n,
            "card_id": card_id,
            "dashboard_tab_id": tab,
            "row": row,
            "col": col,
            "size_x": sx,
            "size_y": sy,
        }
        if parameter_mappings:
            dashcard["parameter_mappings"] = parameter_mappings
        return dashcard

    def _pc_param_mappings(card_id):
        return [
            {
                "card_id": card_id,
                "parameter_id": PC_INCLUDE_PARIS_PARAMETER["id"],
                "target": ["variable", ["template-tag", "include_paris"]],
            }
        ]

    def _head(tab, row, text):
        nonlocal n
        n -= 1
        return _heading(n, tab, row, text)

    def _txt(tab, row, col, sx, sy, text):
        nonlocal n
        n -= 1
        return _text(n, tab, row, col, sx, sy, text)

    r = client.put(
        f"/api/dashboard/{dash_id}",
        json={
            "tabs": [
                {"id": T1, "name": "Ile-de-France"},
                {"id": T2, "name": "Paris"},
                {"id": T3, "name": "Petite couronne"},
            ],
            "parameters": [PC_INCLUDE_PARIS_PARAMETER],
            "dashcards": [
                # ═══ IDF ═══
                _head(T1, 0, "Logement"),
                _card(c_idf_map_prix, T1, 2, 0, 24, 12),
                _card(c_idf_map_loyer, T1, 14, 0, 24, 10),
                _card(c_idf_synthese, T1, 24, 0, 24, 7),
                _card(c_idf_prix, T1, 31, 0, 12, 8),
                _card(c_idf_volume, T1, 31, 12, 12, 8),
                _card(c_idf_ratio, T1, 39, 0, 12, 8),
                _card(c_idf_surface, T1, 39, 12, 12, 8),
                _card(c_idf_loyer_vs_achat, T1, 47, 0, 24, 8),
                _head(T1, 56, "Démographie"),
                _card(c_idf_map_age, T1, 58, 0, 24, 12),
                _card(c_idf_scatter, T1, 70, 0, 24, 10),
                _card(c_idf_map_diplome, T1, 80, 0, 12, 10),
                _card(c_idf_map_sans_diplome, T1, 80, 12, 12, 10),
                _card(c_idf_map_60plus, T1, 90, 0, 24, 10),
                _head(T1, 101, "Sécurité"),
                _card(c_idf_map_delin, T1, 103, 0, 12, 10),
                _card(c_idf_evol_delin, T1, 103, 12, 12, 10),
                _card(c_idf_delin_type, T1, 113, 0, 16, 8),
                _txt(T1, 113, 16, 8, 8, DELIN_LEGEND),
                _card(c_idf_evol_delits, T1, 121, 0, 24, 8),
                _txt(T1, 131, 0, 24, 2, INTRO_TEXT),
                _txt(T1, 134, 0, 24, 5, FEEDBACK_TEXT),
                # ═══ Paris ═══
                _head(T2, 0, "Logement"),
                _card(c_paris_map_prix, T2, 2, 0, 24, 12),
                _card(c_paris_map_loyer_studio, T2, 14, 0, 12, 10),
                _card(c_paris_map_loyer_2p, T2, 14, 12, 12, 10),
                _card(c_paris_evol, T2, 24, 0, 24, 9),
                _card(c_paris_table, T2, 33, 0, 24, 8),
                _card(c_paris_surface, T2, 41, 0, 24, 8),
                _head(T2, 50, "Démographie"),
                _card(c_paris_map_age, T2, 52, 0, 24, 12),
                _card(c_paris_map_diplome, T2, 64, 0, 12, 10),
                _card(c_paris_map_sans_diplome, T2, 64, 12, 12, 10),
                _card(c_paris_map_60plus, T2, 74, 0, 24, 10),
                _head(T2, 85, "Sécurité"),
                _card(c_paris_map_delin, T2, 87, 0, 24, 10),
                _card(c_paris_delin_cat, T2, 97, 0, 16, 10),
                _txt(T2, 97, 16, 8, 10, DELIN_LEGEND),
                _head(T2, 108, "Mobilité"),
                _card(c_paris_map_metro, T2, 110, 0, 12, 10),
                _card(c_paris_map_velib, T2, 110, 12, 12, 10),
                _card(c_paris_map_cyclable, T2, 120, 0, 24, 10),
                _txt(T2, 132, 0, 24, 2, INTRO_TEXT),
                _txt(T2, 135, 0, 24, 5, FEEDBACK_TEXT),
                # ═══ Petite couronne ═══
                _head(T3, 0, "Logement"),
                _card(c_pc_map_prix, T3, 2, 0, 24, 12, _pc_param_mappings(c_pc_map_prix)),
                _card(c_pc_map_loyer, T3, 14, 0, 24, 10, _pc_param_mappings(c_pc_map_loyer)),
                _card(c_pc_evol, T3, 24, 0, 24, 9, _pc_param_mappings(c_pc_evol)),
                _card(c_pc_table, T3, 33, 0, 24, 8, _pc_param_mappings(c_pc_table)),
                _head(T3, 42, "Démographie"),
                _card(c_pc_map_age, T3, 44, 0, 24, 12, _pc_param_mappings(c_pc_map_age)),
                _card(c_pc_map_diplome, T3, 56, 0, 12, 10, _pc_param_mappings(c_pc_map_diplome)),
                _card(c_pc_map_sans_diplome, T3, 56, 12, 12, 10, _pc_param_mappings(c_pc_map_sans_diplome)),
                _card(c_pc_map_60plus, T3, 66, 0, 24, 10, _pc_param_mappings(c_pc_map_60plus)),
                _head(T3, 77, "Sécurité"),
                _card(c_pc_map_delin, T3, 79, 0, 24, 10, _pc_param_mappings(c_pc_map_delin)),
                _card(c_pc_delin_cat, T3, 89, 0, 16, 8, _pc_param_mappings(c_pc_delin_cat)),
                _txt(T3, 89, 16, 8, 8, DELIN_LEGEND),
                _head(T3, 98, "Mobilité"),
                _card(c_pc_map_metro, T3, 100, 0, 12, 10, _pc_param_mappings(c_pc_map_metro)),
                _card(c_pc_map_velib, T3, 100, 12, 12, 10, _pc_param_mappings(c_pc_map_velib)),
                _txt(T3, 112, 0, 24, 2, INTRO_TEXT),
                _txt(T3, 115, 0, 24, 5, FEEDBACK_TEXT),
            ],
        },
    )
    r.raise_for_status()
    print("  3 onglets, 45 cartes, 11 sections")
    return dash_id


# ── Main ────────────────────────────────────────────────────────────


def main() -> None:
    print("\n═══ Setup Metabase — Panorama Ile-de-France ═══\n")

    ensure_geojson_nginx_config()

    print("[1/7] Démarrage PostgreSQL + Metabase")
    start_services()

    print("\n[2/8] Export des marts vers PostgreSQL")
    export_marts_to_postgres()

    print("\n[3/8] Génération GeoJSON")
    generate_geojson()

    # Exports externes (APIs tierces) : non-bloquants car les APIs peuvent
    # refuser les IPs de datacenter ou etre temporairement indisponibles.
    for step, label, fn in [
        ("4/11", "Données Vélib", export_velib_to_postgres),
        ("5/11", "Pistes cyclables", export_cycling_to_postgres),
        ("6/11", "Stations métro et RER (IDFM)", export_metro_to_postgres),
        ("7/11", "Données diplômes INSEE", export_diplomes_to_postgres),
    ]:
        print(f"\n[{step}] {label}")
        try:
            fn()
        except Exception as e:
            print(f"  ⚠ Echec ({type(e).__name__}: {e})")
            print(f"  → Le dashboard sera créé sans ces données.")

    client = httpx.Client(base_url=METABASE_URL, timeout=30)

    print("\n[8/11] Connexion à Metabase")
    wait_for_metabase(client)

    print("\n[9/11] Configuration admin")
    session_id = setup_admin(client)
    client.headers["X-Metabase-Session"] = session_id

    # Si l'instance a ete setup avec un ancien site-name (ex: "France Aujourd'hui"),
    # on le force a la valeur courante. PUT idempotent.
    try:
        client.put("/api/setting/site-name", json={"value": "Panorama Ile-de-France"})
    except Exception:
        pass

    print("\n[10/11] Base de données PostgreSQL")
    db_id = add_postgres_database(client)

    print("\n[11/11] Cartes GeoJSON + dashboard")
    register_geojson_maps(client)
    dash_id = create_tabbed_dashboard(client, db_id)

    print(f"\n{'═' * 55}")
    print(f"  Metabase : {METABASE_URL}")
    print(f"  Dashboard : {METABASE_URL}/dashboard/{dash_id}")
    # On n'affiche plus le mot de passe en clair (prod).
    print(f"  Admin : {ADMIN_EMAIL} (mdp dans .env / MB_ADMIN_PASSWORD)")
    print(f"  3 onglets : Ile-de-France | Paris | Petite couronne")
    print(f"{'═' * 55}\n")

    client.close()


if __name__ == "__main__":
    main()
