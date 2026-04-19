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

# COMPOSE_FILE : fichier compose utilisé pour piloter postgres/metabase depuis l'hôte.
# En mode container (Dokploy pipeline, SKIP_COMPOSE_START=1) on saute cette orchestration :
# postgres et metabase tournent déjà, on se connecte directement en TCP.
COMPOSE_FILE = os.environ.get("COMPOSE_FILE", "docker-compose.yml")
SKIP_COMPOSE_START = os.environ.get("SKIP_COMPOSE_START", "0") == "1"

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
    """Build a `docker compose -f <file> --profile local ...` command.

    Le profil `local` active Caddy (reverse proxy de dev) sans impacter la
    prod : en prod, c'est Dokploy/Traefik qui route, donc le pipeline tourne
    avec SKIP_COMPOSE_START=1 et ne passe jamais par ici.
    """
    base = ["-f", COMPOSE_FILE, "--profile", "local"]
    if shutil.which("docker-compose"):
        return ["docker-compose", *base, *args]
    return ["docker", "compose", *base, *args]


def _wait_postgres_tcp() -> bool:
    """Poll postgres en TCP (via le client psql) jusqu'à ce qu'il réponde."""
    import socket
    for _ in range(60):
        try:
            with socket.create_connection((_pg_host, int(_pg_port)), timeout=2):
                pass
        except OSError:
            print(".", end="", flush=True)
            time.sleep(2)
            continue
        # Port ouvert : vérifier que postgres accepte des requêtes.
        env = {**os.environ, "PGPASSWORD": _pg_password}
        result = subprocess.run(
            ["psql", "-h", _pg_host, "-p", _pg_port, "-U", _pg_user,
             "-d", _pg_db, "-c", "SELECT 1"],
            capture_output=True, env=env,
        )
        if result.returncode == 0:
            return True
        print(".", end="", flush=True)
        time.sleep(2)
    return False


def _ensure_metabase_app_db() -> None:
    """Crée la DB `metabase_app` si elle n'existe pas (idempotent)."""
    env = {**os.environ, "PGPASSWORD": _pg_password}
    subprocess.run(
        ["psql", "-h", _pg_host, "-p", _pg_port, "-U", _pg_user,
         "-d", _pg_db,
         "-c", f"CREATE DATABASE metabase_app OWNER {_pg_user};"],
        capture_output=True, env=env,
    )


def start_services() -> None:
    """Démarre (ou attend) PostgreSQL + Metabase.

    - Mode dev (défaut) : `docker compose up -d` depuis l'hôte.
    - Mode container (SKIP_COMPOSE_START=1) : les services tournent déjà, on
      attend juste que postgres soit joignable en TCP.
    """
    if SKIP_COMPOSE_START:
        print("  Mode container : skip compose up, attente TCP postgres…", end="", flush=True)
        if not _wait_postgres_tcp():
            print(" TIMEOUT")
            sys.exit(1)
        print(" OK")
        _ensure_metabase_app_db()
        return

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
        SELECT c.code_commune, c.nom_commune as nom_commune,
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

    # Une ligne par (station, ligne) — permet de compter les lignes uniques
    # qui desservent une commune (et pas la somme des lignes × stations, qui
    # gonfle artificiellement les arrondissements denses comme Gare du Nord).
    con.execute(
        "CREATE TABLE stops (name VARCHAR, lat DOUBLE, lon DOUBLE, line VARCHAR)"
    )
    con.executemany(
        "INSERT INTO stops VALUES (?, ?, ?, ?)",
        [
            (n, d["lat"], d["lon"], line)
            for n, d in stations.items()
            for line in d["lines"]
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
        station_in_commune AS (
            SELECT DISTINCT s.name, s.line,
                   g.code as code_commune, g.nom as nom_commune
            FROM stops s
            JOIN geo g ON ST_Contains(g.geom, ST_Point(s.lon, s.lat))
        )
        SELECT sc.code_commune, sc.nom_commune,
               count(DISTINCT sc.name)::integer as nb_stations,
               count(DISTINCT sc.line)::integer as nb_lignes_uniques,
               round(a.area_km2::numeric, 2) as superficie_km2,
               round((count(DISTINCT sc.name) / a.area_km2)::numeric, 1)
                   as stations_par_km2
        FROM station_in_commune sc
        JOIN areas a ON sc.code_commune = a.code
        GROUP BY sc.code_commune, sc.nom_commune, a.area_km2
        ORDER BY sc.code_commune
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
        "idf_departements.geojson",
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

    def simplify_coords(coords, precision=4):
        if isinstance(coords[0], (int, float)):
            return [round(c, precision) for c in coords]
        return [simplify_coords(c, precision) for c in coords]

    def write_departements():
        dep_path = GEOJSON_DIR / "idf_departements.geojson"
        if dep_path.exists():
            return
        dep_names = {
            "75": "Paris",
            "77": "Seine-et-Marne",
            "78": "Yvelines",
            "91": "Essonne",
            "92": "Hauts-de-Seine",
            "93": "Seine-Saint-Denis",
            "94": "Val-de-Marne",
            "95": "Val-d'Oise",
        }
        communes_path = str(GEOJSON_DIR / "idf_communes.geojson")
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        # Buffer légèrement avant l'union pour fermer les slivers entre communes
        # voisines (sans quoi on voit toutes les limites communales), puis on
        # rétracte et on simplifie le résultat.
        con.execute(
            f"CREATE TABLE c AS SELECT substr(code, 1, 2) as code_dep, "
            f"ST_Buffer(geom, 0.0005) as geom FROM ST_Read('{communes_path}')"
        )
        dep_features = []
        for code_dep in sorted(dep_names):
            geom_json = con.execute(
                "SELECT ST_AsGeoJSON(ST_Simplify("
                "  ST_Buffer(ST_Union_Agg(geom), -0.0005), 0.002)) "
                "FROM c WHERE code_dep = ?",
                [code_dep],
            ).fetchone()[0]
            if not geom_json:
                continue
            geom = json.loads(geom_json)
            geom["coordinates"] = simplify_coords(geom["coordinates"])
            dep_features.append(
                {
                    "type": "Feature",
                    "geometry": geom,
                    "properties": {
                        "code": code_dep,
                        "nom": dep_names[code_dep],
                    },
                }
            )
        con.close()
        write_geo("idf_departements", dep_features)

    idf_path = GEOJSON_DIR / "idf_communes.geojson"
    if idf_path.exists():
        features = json.loads(idf_path.read_text(encoding="utf-8"))["features"]
        write_zone_files(features)
        write_departements()
        return

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
    write_departements()


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
                "site_name": "Panorama Île-de-France",
            },
            "prefs": {
                "site_name": "Panorama Île-de-France",
                "site_locale": "fr",
                "allow_tracking": False,
            },
        },
    )
    # Metabase renvoie 400 ou 403 si le setup-token persiste mais que l'admin
    # est déjà créé (container restart, reuse de volume, etc.). Dans ces cas
    # on bascule sur un login classique. On logue le body pour diagnostiquer
    # quand les deux échouent (ex: password policy).
    if r.status_code in (400, 403):
        print(f"  Setup déjà fait (HTTP {r.status_code}) — body: {r.text[:300]}")
        print("  Tentative de connexion avec les credentials fournis…")
        r = client.post(
            "/api/session",
            json={
                "username": ADMIN_EMAIL,
                "password": ADMIN_PASSWORD,
            },
        )
        if r.status_code != 200:
            print(f"  Échec login (HTTP {r.status_code}) — body: {r.text[:300]}")
        r.raise_for_status()
        return r.json()["id"]
    r.raise_for_status()
    return r.json()["id"]


def add_postgres_database(client: httpx.Client) -> int:
    """Add PostgreSQL database connection to Metabase (or rename legacy entry)."""
    target_name = "Panorama Île-de-France"
    legacy_names = {"France Aujourd'hui", "Panorama Ile-de-France"}  # migration douce

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
        ("idf_communes", "Communes Île-de-France"),
        ("idf_departements", "Départements Île-de-France"),
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
    print("  6 cartes GeoJSON enregistrées")


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


def map_viz(
    region: str,
    metric: str,
    colors: list[str] | None = None,
    dimension: str = "Code commune",
) -> dict:
    viz = {
        "map.type": "region",
        "map.region": region,
        "map.dimension": dimension,
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
**Sources :** Immobilier DVF 2020-2025 (Cerema) · Revenus, démographie, éducation RP 2021 (INSEE) · Loyers 2025 (ANIL) · Délinquance 2016-2024 (SSMSI) · Mobilité (IDFM, Paris OpenData)"""

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
    dashboard_names = ("Panorama Île-de-France", "Panorama Ile-de-France")
    r = client.get("/api/dashboard/")
    items = r.json() if r.status_code == 200 else []
    if not isinstance(items, list):
        items = []
    for item in items:
        if not isinstance(item, dict) or item.get("archived"):
            continue
        if dash_id is None and item.get("name") in dashboard_names:
            dash_id = item["id"]
            print(f"  Dashboard existant (id={dash_id}), mise a jour…")
        else:
            client.delete(f"/api/dashboard/{item['id']}")
            print(f"  Ancien dashboard {item['id']} supprime")

    # ── IDF tab cards ──
    c_idf_map_prix = make_card(
        client,
        db_id,
        "Prix au m² — Île-de-France",
        "map",
        f"""SELECT code_departement AS "Département",
       round((sum(prix_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0))::numeric) AS "Prix au m² (€)",
       sum(nb_ventes)::integer AS "Nombre de ventes"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y}
GROUP BY "Département" ORDER BY "Département"
""",
        f"Prix médian au m² par département (pondéré par le volume de ventes). Source : DVF {Y} (Cerema).",
        map_viz("idf_departements", "Prix au m² (€)", dimension="Département"),
    )

    c_idf_map_age = make_card(
        client,
        db_id,
        "Part des 25–39 ans — Île-de-France",
        "map",
        f"""SELECT code_departement AS "Département",
       round((sum(part_25_39 * population_2021) / nullif(sum(population_2021), 0) * 100)::numeric, 1) AS "Part des 25–39 ans (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND part_25_39 IS NOT NULL
GROUP BY "Département" ORDER BY "Département"
""",
        "Part des 25–39 ans dans la population, par département. Source : RP 2021 (INSEE).",
        map_viz("idf_departements", "Part des 25–39 ans (%)", dimension="Département"),
    )

    c_idf_synthese = make_card(
        client,
        db_id,
        "Synthèse par zone",
        "table",
        """SELECT zone_idf AS "Zone",
       annee::text AS "Année",
       nb_communes AS "Nombre de communes",
       nb_ventes_total AS "Nombre de ventes",
       round(prix_m2_median_pondere::numeric) AS "Prix au m² (€)",
       round(niveau_vie_median_pondere::numeric) AS "Niveau de vie médian (€)",
       round(ratio_achat_revenu_pondere::numeric, 1) AS "Ratio prix / revenu (années)",
       round((part_25_39_ponderee * 100)::numeric, 1) AS "Part des 25–39 ans (%)"
FROM mart_immo__synthese_zone ORDER BY "Année" DESC, "Zone" """,
        "Agrégats pondérés par zone. Prix DVF 2020-2025, revenus Filosofi 2021, démographie RP 2021.",
    )

    c_idf_prix = make_card(
        client,
        db_id,
        "Évolution du prix au m² par zone",
        "line",
        """SELECT annee::text AS "Année", zone_idf AS "Zone", round(prix_m2_median_pondere::numeric) AS "Prix au m² (€)"
FROM mart_immo__synthese_zone ORDER BY "Année", "Zone"
""",
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["Année", "Zone"],
            "graph.metrics": ["Prix au m² (€)"],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "€ / m²",
        },
    )

    c_idf_volume = make_card(
        client,
        db_id,
        "Nombre de ventes par zone",
        "bar",
        """SELECT annee::text AS "Année",
       zone_idf AS "Zone",
       nb_ventes_total AS "Nombre de ventes"
FROM mart_immo__synthese_zone ORDER BY "Année", "Zone" """,
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["Année", "Zone"],
            "graph.metrics": ["Nombre de ventes"],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "Nombre de ventes",
            "stackable.stack_type": "stacked",
        },
    )

    c_idf_scatter = make_card(
        client,
        db_id,
        f"Part des 25–39 ans vs prix au m² par département ({Y})",
        "scatter",
        f"""SELECT code_departement AS "Département",
       round((sum(prix_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0))::numeric) AS "Prix au m² (€)",
       round((sum(part_25_39 * population_2021) / nullif(sum(population_2021), 0) * 100)::numeric, 1) AS "Part des 25–39 ans (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND part_25_39 IS NOT NULL
GROUP BY "Département" ORDER BY "Département"
""",
        f"Chaque point = un département. Sources : prix DVF {Y} (Cerema), démographie RP 2021 (INSEE).",
        viz={
            "graph.dimensions": ["Prix au m² (€)", "Département"],
            "graph.metrics": ["Part des 25–39 ans (%)"],
            "graph.x_axis.title_text": "Prix au m² (€)",
            "graph.y_axis.title_text": "Part des 25–39 ans (%)",
        },
    )

    c_idf_ratio = make_card(
        client,
        db_id,
        "Années de revenu pour un achat médian",
        "line",
        """SELECT annee::text AS "Année", zone_idf AS "Zone",
       round(ratio_achat_revenu_pondere::numeric, 1) AS "Ratio prix / revenu (années)"
FROM mart_immo__synthese_zone ORDER BY "Année", "Zone"
""",
        "Années de revenu (Filosofi 2021) pour un achat médian (DVF 2020-2025).",
        viz={
            "graph.dimensions": ["Année", "Zone"],
            "graph.metrics": ["Ratio prix / revenu (années)"],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "Années de revenu",
        },
    )

    c_idf_surface = make_card(
        client,
        db_id,
        "Surface médiane par zone",
        "line",
        """SELECT annee::text AS "Année", zone_idf AS "Zone",
       round(sum(surface_mediane * nb_ventes) / nullif(sum(nb_ventes), 0)) AS "Surface médiane (m²)"
FROM mart_immo__accessibilite_commune
GROUP BY "Année", "Zone" ORDER BY "Année", "Zone"
""",
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["Année", "Zone"],
            "graph.metrics": ["Surface médiane (m²)"],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "Surface médiane (m²)",
        },
    )

    c_idf_map_loyer = make_card(
        client,
        db_id,
        "Loyer au m² — Île-de-France",
        "map",
        f"""SELECT code_departement AS "Département",
       round((sum(loyer_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0))::numeric, 1) AS "Loyer au m² (€)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND loyer_m2_median IS NOT NULL
GROUP BY "Département" ORDER BY "Département"
""",
        "Loyer prédit au m² (appartements), par département. Source : Carte des loyers 2025 (ANIL).",
        map_viz("idf_departements", "Loyer au m² (€)", dimension="Département"),
    )

    c_idf_loyer_vs_achat = make_card(
        client,
        db_id,
        "Loyer vs mensualité d'achat au m² par zone",
        "bar",
        f"""SELECT zone_idf AS "Zone",
       round((sum(loyer_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0))::numeric, 1) AS "Loyer au m² (€/mois)",
       round((sum(prix_m2_median * nb_ventes) / nullif(sum(nb_ventes), 0) / 12)::numeric, 0) AS "Mensualité d'achat au m² (€)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND loyer_m2_median IS NOT NULL
GROUP BY "Zone" ORDER BY "Zone" """,
        f"Loyer mensuel ANIL 2025 vs mensualité d'achat (1/12 du prix DVF {Y}).",
        viz={
            "graph.dimensions": ["Zone"],
            "graph.metrics": ["Loyer au m² (€/mois)", "Mensualité d'achat au m² (€)"],
            "graph.y_axis.title_text": "€ / m²",
        },
    )

    c_idf_map_delin = make_card(
        client,
        db_id,
        "Délinquance — Île-de-France",
        "map",
        """SELECT code_departement AS "Département",
       round((sum(nb_faits_delinquance::numeric) / nullif(sum(population_2021), 0) * 1000)::numeric, 1) AS "Faits pour 1 000 hab."
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND taux_delinquance_pour_mille IS NOT NULL
GROUP BY "Département" ORDER BY "Département"
""",
        "Faits pour 1000 habitants, dernière année disponible, par département. Source : SSMSI / Min. Intérieur (2016-2024).",
        map_viz("idf_departements", "Faits pour 1 000 hab.", dimension="Département"),
    )

    c_idf_evol_delin = make_card(
        client,
        db_id,
        "Évolution de la délinquance par zone",
        "line",
        """SELECT annee::text AS "Année", zone_idf AS "Zone",
       round((sum(nb_faits_delinquance::numeric) / nullif(sum(population_2021), 0) * 1000)::numeric, 1) AS "Faits pour 1 000 hab."
FROM mart_immo__accessibilite_commune
WHERE taux_delinquance_pour_mille IS NOT NULL
GROUP BY "Année", "Zone" ORDER BY "Année", "Zone"
""",
        "Source : SSMSI / Min. Intérieur (2016-2024).",
        viz={
            "graph.dimensions": ["Année", "Zone"],
            "graph.metrics": ["Faits pour 1 000 hab."],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "Faits pour 1 000 hab.",
        },
    )

    c_idf_delin_type = make_card(
        client,
        db_id,
        "Délinquance par catégorie — Île-de-France",
        "bar",
        f"""SELECT {DELIN_CATEGORIES} AS "Catégorie",
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) AS "Faits pour 1 000 hab."
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE d.annee = 2024 AND a.zone_idf IS NOT NULL
GROUP BY "Catégorie" ORDER BY "Faits pour 1 000 hab." DESC""",
        "Source : SSMSI / Min. Intérieur (année 2024).",
        viz={
            "graph.dimensions": ["Catégorie"],
            "graph.metrics": ["Faits pour 1 000 hab."],
            "graph.y_axis.title_text": "Faits pour 1 000 hab.",
        },
    )

    c_idf_evol_delits = make_card(
        client,
        db_id,
        "Évolution des délits par catégorie — Île-de-France",
        "line",
        f"""SELECT d.annee::text AS "Année", {DELIN_CATEGORIES} AS "Catégorie",
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) as taux
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE a.zone_idf IS NOT NULL
GROUP BY d.annee, "Catégorie" ORDER BY d.annee, "Catégorie"
""",
        "Source : SSMSI / Min. Intérieur (2016-2024).",
        viz={
            "graph.dimensions": ["Année", "Catégorie"],
            "graph.metrics": ["taux"],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "Faits pour 1 000 hab.",
        },
    )

    c_idf_map_60plus = make_card(
        client,
        db_id,
        "Part des 60 ans et plus — Île-de-France",
        "map",
        f"""SELECT code_departement AS "Département",
       round((sum(part_60_plus * population_2021) / nullif(sum(population_2021), 0) * 100)::numeric, 1) AS "Part des 60 ans et plus (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND part_60_plus IS NOT NULL
GROUP BY "Département" ORDER BY "Département"
""",
        "Part des 60 ans et plus dans la population, par département. Source : RP 2021 (INSEE).",
        map_viz("idf_departements", "Part des 60 ans et plus (%)", dimension="Département"),
    )

    c_idf_map_diplome = make_card(
        client,
        db_id,
        "Part des diplômés du supérieur — Île-de-France",
        "map",
        f"""SELECT a.code_departement AS "Département",
       round((sum(d.part_etudes_sup * d.pop_15p_non_scol) / nullif(sum(d.pop_15p_non_scol), 0))::numeric, 1) AS "Diplômés du supérieur (%)"
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND d.part_etudes_sup IS NOT NULL
GROUP BY a.code_departement ORDER BY a.code_departement""",
        "Part de la population 15+ ayant un diplôme bac+2 et plus, par département. Source : RP 2021 (INSEE).",
        map_viz("idf_departements", "Diplômés du supérieur (%)", dimension="Département"),
    )

    c_idf_map_sans_diplome = make_card(
        client,
        db_id,
        "Part sans diplôme — Île-de-France",
        "map",
        f"""SELECT a.code_departement AS "Département",
       round((sum(d.part_sans_diplome * d.pop_15p_non_scol) / nullif(sum(d.pop_15p_non_scol), 0))::numeric, 1) AS "Sans diplôme (%)"
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND d.part_sans_diplome IS NOT NULL
GROUP BY a.code_departement ORDER BY a.code_departement""",
        "Part de la population 15+ sans diplôme ou avec CEP, par département. Source : RP 2021 (INSEE).",
        map_viz("idf_departements", "Sans diplôme (%)", dimension="Département"),
    )

    # ── Paris tab cards ──
    c_paris_map_prix = make_card(
        client,
        db_id,
        "Prix au m² — Paris",
        "map",
        f"""SELECT code_commune AS "Code commune", round(prix_m2_median::numeric) AS "Prix au m² (€)"
FROM mart_immo__accessibilite_commune WHERE annee = {Y} AND code_commune LIKE '751%'""",
        f"Prix médian au m² par arrondissement. Source : DVF {Y} (Cerema).",
        map_viz("paris_arr", "Prix au m² (€)"),
    )

    c_paris_map_age = make_card(
        client,
        db_id,
        "Part des 25–39 ans — Paris",
        "map",
        f"""SELECT code_commune AS "Code commune", round((part_25_39 * 100)::numeric, 1) AS "Part des 25–39 ans (%)"
FROM mart_immo__accessibilite_commune WHERE annee = {Y} AND code_commune LIKE '751%'""",
        "Part des 25–39 ans dans la population. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "Part des 25–39 ans (%)"),
    )

    c_paris_evol = make_card(
        client,
        db_id,
        "Évolution du prix au m² par arrondissement",
        "line",
        """SELECT annee::text AS "Année", nom_commune AS "Commune", round(prix_m2_median::numeric) AS "Prix au m² (€)"
FROM mart_immo__evolution_prix WHERE code_commune LIKE '751%'
ORDER BY "Année", nom_commune""",
        "Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["Année", "Commune"],
            "graph.metrics": ["Prix au m² (€)"],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "€ / m²",
        },
    )

    c_paris_table = make_card(
        client,
        db_id,
        f"Détail par arrondissement ({Y})",
        "table",
        f"""SELECT nom_commune AS "Commune",
       round(prix_m2_median::numeric) AS "Prix au m² (€)",
       nb_ventes AS "Nombre de ventes",
       round((part_25_39 * 100)::numeric, 1) AS "Part des 25–39 ans (%)",
       round(niveau_vie_median::numeric) AS "Revenu médian (€)",
       round(surface_mediane::numeric, 1) AS "Surface médiane (m²)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY prix_m2_median DESC""",
        f"Sources : prix et surface DVF {Y} (Cerema), démographie RP 2021, revenus Filosofi 2021 (INSEE).",
    )

    c_paris_surface = make_card(
        client,
        db_id,
        f"Surface médiane par arrondissement ({Y})",
        "bar",
        f"""SELECT nom_commune AS "Commune",
       round(surface_mediane::numeric, 1) AS "Surface médiane (m²)",
       nb_ventes AS "Nombre de ventes"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' ORDER BY surface_mediane DESC""",
        f"Source : DVF {Y} (Cerema).",
        viz={
            "graph.dimensions": ["Commune"],
            "graph.metrics": ["Surface médiane (m²)"],
            "graph.y_axis.title_text": "Surface médiane (m²)",
            "graph.colors": ["#66BB6A"],
        },
    )

    c_paris_map_loyer_studio = make_card(
        client,
        db_id,
        "Loyer estimé studio — Paris",
        "map",
        f"""SELECT a.code_commune AS "Code commune",
       round((a.loyer_m2_median * p.surface_mediane)::numeric) AS "Loyer mensuel (€)"
FROM mart_immo__accessibilite_commune a
JOIN prix_paris_par_pieces p
    ON a.code_commune = p.code_commune AND a.annee = p.annee
WHERE a.annee = {Y} AND p.nb_pieces = 1 AND a.loyer_m2_median IS NOT NULL""",
        f"Loyer m² ANIL 2025 × surface médiane studio DVF {Y}.",
        map_viz("paris_arr", "Loyer mensuel (€)"),
    )

    c_paris_map_loyer_2p = make_card(
        client,
        db_id,
        "Loyer estimé 2 pièces — Paris",
        "map",
        f"""SELECT a.code_commune AS "Code commune",
       round((a.loyer_m2_median * p.surface_mediane)::numeric) AS "Loyer mensuel (€)"
FROM mart_immo__accessibilite_commune a
JOIN prix_paris_par_pieces p
    ON a.code_commune = p.code_commune AND a.annee = p.annee
WHERE a.annee = {Y} AND p.nb_pieces = 2 AND a.loyer_m2_median IS NOT NULL""",
        f"Loyer m² ANIL 2025 × surface médiane 2P DVF {Y}.",
        map_viz("paris_arr", "Loyer mensuel (€)"),
    )

    c_paris_map_delin = make_card(
        client,
        db_id,
        "Délinquance — Paris",
        "map",
        """SELECT code_commune AS "Code commune",
       round(taux_delinquance_pour_mille::numeric, 1) AS "Faits pour 1 000 hab."
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND code_commune LIKE '751%' AND taux_delinquance_pour_mille IS NOT NULL""",
        "Faits pour 1000 habitants, dernière année disponible. Source : SSMSI / Min. Intérieur (2016-2024).",
        map_viz("paris_arr", "Faits pour 1 000 hab."),
    )

    c_paris_delin_cat = make_card(
        client,
        db_id,
        "Délinquance par catégorie et arrondissement",
        "bar",
        f"""SELECT cast(right(d.code_commune, 2) as integer) AS "Arrondissement",
       {DELIN_CATEGORIES} AS "Catégorie",
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) AS "Faits pour 1 000 hab."
FROM stg_delinquance_detail d
WHERE d.annee = 2024 AND d.code_commune LIKE '751%'
GROUP BY "Arrondissement", "Catégorie" ORDER BY "Arrondissement", "Faits pour 1 000 hab." DESC""",
        "Source : SSMSI / Min. Intérieur (année 2024).",
        viz={
            "graph.dimensions": ["Arrondissement", "Catégorie"],
            "graph.metrics": ["Faits pour 1 000 hab."],
            "graph.y_axis.title_text": "Faits pour 1 000 hab.",
            "stackable.stack_type": "stacked",
        },
    )

    c_paris_map_cyclable = make_card(
        client,
        db_id,
        "Densité pistes cyclables — Paris (km/km²)",
        "map",
        """SELECT code_commune AS "Code commune",
       km_par_km2 AS "Pistes cyclables (km/km²)"
FROM cyclable_paris""",
        "Linéaire d'aménagements cyclables par km². Source : Paris OpenData (snapshot).",
        map_viz("paris_arr", "Pistes cyclables (km/km²)"),
    )

    c_paris_map_metro = make_card(
        client,
        db_id,
        "Densité métro + RER — Paris (stations/km²)",
        "map",
        """SELECT code_commune AS "Code commune",
       stations_par_km2 AS "Stations par km²",
       nb_lignes_uniques AS "Lignes desservies"
FROM metro_stations WHERE code_commune LIKE '751%'""",
        "Stations métro et RER par km² (uniques par nom). Source : IDFM Open Data.",
        map_viz("paris_arr", "Stations par km²"),
    )

    c_paris_map_60plus = make_card(
        client,
        db_id,
        "Part des 60 ans et plus — Paris",
        "map",
        f"""SELECT code_commune AS "Code commune",
       round((part_60_plus * 100)::numeric, 1) AS "Part des 60 ans et plus (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' AND part_60_plus IS NOT NULL""",
        "Part des 60 ans et plus dans la population. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "Part des 60 ans et plus (%)"),
    )

    c_paris_map_diplome = make_card(
        client,
        db_id,
        "Part des diplômés du supérieur — Paris",
        "map",
        f"""SELECT a.code_commune AS "Code commune",
       d.part_etudes_sup AS "Diplômés du supérieur (%)"
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND a.code_commune LIKE '751%'""",
        "Part de la population 15+ ayant un diplôme bac+2 et plus. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "Diplômés du supérieur (%)"),
    )

    c_paris_map_sans_diplome = make_card(
        client,
        db_id,
        "Part sans diplôme — Paris",
        "map",
        f"""SELECT a.code_commune AS "Code commune",
       d.part_sans_diplome AS "Sans diplôme (%)"
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND a.code_commune LIKE '751%'""",
        "Part de la population 15+ sans diplôme ou avec CEP. Source : RP 2021 (INSEE).",
        map_viz("paris_arr", "Sans diplôme (%)"),
    )

    c_paris_scatter = make_card(
        client,
        db_id,
        f"Part des 25–39 ans vs prix au m² par arrondissement ({Y})",
        "scatter",
        f"""SELECT nom_commune AS "Commune",
       round(prix_m2_median::numeric) AS "Prix au m² (€)",
       round((part_25_39 * 100)::numeric, 1) AS "Part des 25–39 ans (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND code_commune LIKE '751%' AND part_25_39 IS NOT NULL
ORDER BY prix_m2_median""",
        f"Chaque point = un arrondissement. Sources : prix DVF {Y} (Cerema), démographie RP 2021 (INSEE).",
        viz={
            "graph.dimensions": ["Prix au m² (€)"],
            "graph.metrics": ["Part des 25–39 ans (%)"],
            "graph.x_axis.title_text": "Prix au m² (€)",
            "graph.y_axis.title_text": "Part des 25–39 ans (%)",
        },
    )

    # ── Petite couronne tab cards ──
    c_pc_map_prix = make_card(
        client,
        db_id,
        "Prix au m² — Petite couronne",
        "map",
        f"""SELECT code_commune AS "Code commune", round(prix_m2_median::numeric) AS "Prix au m² (€)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}""",
        f"Prix médian au m² par commune. Source : DVF {Y} (Cerema).",
        map_viz("petite_couronne_plus_paris", "Prix au m² (€)"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_age = make_card(
        client,
        db_id,
        "Part des 25–39 ans — Petite couronne",
        "map",
        f"""SELECT code_commune AS "Code commune", round((part_25_39 * 100)::numeric, 1) AS "Part des 25–39 ans (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}""",
        "Part des 25–39 ans dans la population. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "Part des 25–39 ans (%)"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_evol = make_card(
        client,
        db_id,
        "Évolution du prix au m² — top 10 communes",
        "line",
        f"""SELECT e.annee::text AS "Année", e.nom_commune AS "Commune", round(e.prix_m2_median::numeric) AS "Prix au m² (€)"
FROM mart_immo__evolution_prix e
WHERE {_pc_scope(dept_field='e.code_departement')}
  AND e.code_commune IN (
    SELECT code_commune FROM mart_immo__accessibilite_commune
    WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}
    ORDER BY nb_ventes DESC LIMIT 10)
ORDER BY "Année", nom_commune""",
        "Top 10 communes par volume de ventes. Source : DVF 2020-2025 (Cerema).",
        viz={
            "graph.dimensions": ["Année", "Commune"],
            "graph.metrics": ["Prix au m² (€)"],
            "graph.x_axis.title_text": "Année",
            "graph.y_axis.title_text": "€ / m²",
        },
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_table = make_card(
        client,
        db_id,
        f"Top 20 communes ({Y})",
        "table",
        f"""SELECT nom_commune AS "Commune", code_departement AS "Département",
       round(prix_m2_median::numeric) AS "Prix au m² (€)",
       nb_ventes AS "Nombre de ventes",
       round((part_25_39 * 100)::numeric, 1) AS "Part des 25–39 ans (%)",
       round(niveau_vie_median::numeric) AS "Revenu médian (€)",
       round(ratio_achat_revenu_annuel::numeric, 1) AS "Ratio prix / revenu (années)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}
ORDER BY "Nombre de ventes" DESC LIMIT 20""",
        f"Classées par volume de ventes. Sources : DVF {Y}, RP 2021, Filosofi 2021.",
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_loyer = make_card(
        client,
        db_id,
        "Loyer au m² — Petite couronne",
        "map",
        f"""SELECT code_commune AS "Code commune", round(loyer_m2_median::numeric, 1) AS "Loyer au m² (€)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y}
  AND {_pc_scope('zone_idf', 'code_departement')}
  AND loyer_m2_median IS NOT NULL""",
        "Loyer prédit au m² (appartements). Source : Carte des loyers 2025 (ANIL).",
        map_viz("petite_couronne_plus_paris", "Loyer au m² (€)"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_delin = make_card(
        client,
        db_id,
        "Délinquance — Petite couronne",
        "map",
        f"""SELECT code_commune AS "Code commune",
       round(taux_delinquance_pour_mille::numeric, 1) AS "Faits pour 1 000 hab."
FROM mart_immo__accessibilite_commune
WHERE annee = (SELECT max(annee) FROM mart_immo__accessibilite_commune
               WHERE taux_delinquance_pour_mille IS NOT NULL)
  AND {_pc_scope('zone_idf', 'code_departement')}
  AND taux_delinquance_pour_mille IS NOT NULL""",
        "Faits pour 1000 habitants, dernière année disponible. Source : SSMSI / Min. Intérieur (2016-2024).",
        map_viz("petite_couronne_plus_paris", "Faits pour 1 000 hab."),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_delin_cat = make_card(
        client,
        db_id,
        "Délinquance par catégorie — Petite couronne",
        "bar",
        f"""SELECT {DELIN_CATEGORIES} AS "Catégorie",
       round((sum(d.nb_faits) / nullif(sum(d.population), 0) * 1000)::numeric, 1) AS "Faits pour 1 000 hab."
FROM stg_delinquance_detail d
INNER JOIN mart_immo__accessibilite_commune a
    ON d.code_commune = a.code_commune AND d.annee = a.annee
WHERE d.annee = 2024 AND {_pc_scope('a.zone_idf', 'a.code_departement')}
GROUP BY "Catégorie" ORDER BY "Faits pour 1 000 hab." DESC""",
        "Source : SSMSI / Min. Intérieur (année 2024).",
        viz={
            "graph.dimensions": ["Catégorie"],
            "graph.metrics": ["Faits pour 1 000 hab."],
            "graph.y_axis.title_text": "Faits pour 1 000 hab.",
        },
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_metro = make_card(
        client,
        db_id,
        "Densité métro + RER — Petite couronne (stations/km²)",
        "map",
        f"""SELECT code_commune AS "Code commune",
       stations_par_km2 AS "Stations par km²"
FROM metro_stations
WHERE {_pc_scope(code_field='code_commune')}""",
        "Stations métro et RER par km². Source : IDFM Open Data.",
        map_viz("petite_couronne_plus_paris", "Stations par km²"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_60plus = make_card(
        client,
        db_id,
        "Part des 60 ans et plus — Petite couronne",
        "map",
        f"""SELECT code_commune AS "Code commune",
       round((part_60_plus * 100)::numeric, 1) AS "Part des 60 ans et plus (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y}
  AND {_pc_scope('zone_idf', 'code_departement')}
  AND part_60_plus IS NOT NULL""",
        "Part des 60 ans et plus dans la population. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "Part des 60 ans et plus (%)"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_diplome = make_card(
        client,
        db_id,
        "Part des diplômés du supérieur — Petite couronne",
        "map",
        f"""SELECT a.code_commune AS "Code commune",
       d.part_etudes_sup AS "Diplômés du supérieur (%)"
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND {_pc_scope('a.zone_idf', 'a.code_departement')}""",
        "Part de la population 15+ ayant un diplôme bac+2 et plus. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "Diplômés du supérieur (%)"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_map_sans_diplome = make_card(
        client,
        db_id,
        "Part sans diplôme — Petite couronne",
        "map",
        f"""SELECT a.code_commune AS "Code commune",
       d.part_sans_diplome AS "Sans diplôme (%)"
FROM mart_immo__accessibilite_commune a
JOIN diplomes_communes d ON a.code_commune = d.code_commune
WHERE a.annee = {Y} AND {_pc_scope('a.zone_idf', 'a.code_departement')}""",
        "Part de la population 15+ sans diplôme ou avec CEP. Source : RP 2021 (INSEE).",
        map_viz("petite_couronne_plus_paris", "Sans diplôme (%)"),
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    c_pc_scatter = make_card(
        client,
        db_id,
        f"Part des 25–39 ans vs prix au m² par commune ({Y})",
        "scatter",
        f"""SELECT nom_commune AS "Commune",
       code_departement AS "Département",
       round(prix_m2_median::numeric) AS "Prix au m² (€)",
       round((part_25_39 * 100)::numeric, 1) AS "Part des 25–39 ans (%)"
FROM mart_immo__accessibilite_commune
WHERE annee = {Y} AND {_pc_scope('zone_idf', 'code_departement')}
  AND part_25_39 IS NOT NULL
ORDER BY prix_m2_median""",
        f"Chaque point = une commune. Sources : prix DVF {Y} (Cerema), démographie RP 2021 (INSEE).",
        viz={
            "graph.dimensions": ["Prix au m² (€)", "Département"],
            "graph.metrics": ["Part des 25–39 ans (%)"],
            "graph.x_axis.title_text": "Prix au m² (€)",
            "graph.y_axis.title_text": "Part des 25–39 ans (%)",
        },
        template_tags=PC_INCLUDE_PARIS_TAG,
    )

    # ── Create dashboard if needed ──
    if dash_id is None:
        r = client.post(
            "/api/dashboard",
            json={
                "name": "Panorama Île-de-France",
                "description": "Immobilier, revenus, loyers, démographie et sécurité par commune (2020–2025).",
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

    tabs_spec = [
        {"id": T2, "name": "Paris"},
        {"id": T3, "name": "Petite couronne"},
        {"id": T1, "name": "Île-de-France"},
    ]
    dashcards = [
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
                _card(c_paris_scatter, T2, 84, 0, 24, 10),
                _head(T2, 95, "Sécurité"),
                _card(c_paris_map_delin, T2, 97, 0, 24, 10),
                _card(c_paris_delin_cat, T2, 107, 0, 16, 10),
                _txt(T2, 107, 16, 8, 10, DELIN_LEGEND),
                _head(T2, 118, "Mobilité"),
                _card(c_paris_map_metro, T2, 120, 0, 24, 10),
                _card(c_paris_map_cyclable, T2, 130, 0, 24, 10),
                _txt(T2, 142, 0, 24, 2, INTRO_TEXT),
                _txt(T2, 145, 0, 24, 5, FEEDBACK_TEXT),
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
                _card(c_pc_scatter, T3, 76, 0, 24, 10, _pc_param_mappings(c_pc_scatter)),
                _head(T3, 87, "Sécurité"),
                _card(c_pc_map_delin, T3, 89, 0, 24, 10, _pc_param_mappings(c_pc_map_delin)),
                _card(c_pc_delin_cat, T3, 99, 0, 16, 8, _pc_param_mappings(c_pc_delin_cat)),
                _txt(T3, 99, 16, 8, 8, DELIN_LEGEND),
                _head(T3, 108, "Mobilité"),
                _card(c_pc_map_metro, T3, 110, 0, 24, 10, _pc_param_mappings(c_pc_map_metro)),
                _txt(T3, 122, 0, 24, 2, INTRO_TEXT),
                _txt(T3, 125, 0, 24, 5, FEEDBACK_TEXT),
            ]

    r = client.put(
        f"/api/dashboard/{dash_id}",
        json={
            "name": "Panorama Île-de-France",
            "description": "Immobilier, revenus, loyers, démographie et sécurité par commune (2020–2025).",
            "tabs": tabs_spec,
            "parameters": [PC_INCLUDE_PARIS_PARAMETER],
            "dashcards": dashcards,
        },
    )
    r.raise_for_status()
    print("  3 onglets, 45 cartes, 11 sections")
    return dash_id


# ── Main ────────────────────────────────────────────────────────────


def main() -> None:
    print("\n═══ Setup Metabase — Panorama Ile-de-France ═══\n")

    ensure_geojson_nginx_config()

    print("[1/10] Démarrage PostgreSQL + Metabase")
    start_services()

    print("\n[2/10] Export des marts vers PostgreSQL")
    export_marts_to_postgres()

    print("\n[3/10] Génération GeoJSON")
    generate_geojson()

    # Exports externes (APIs tierces) : non-bloquants car les APIs peuvent
    # refuser les IPs de datacenter ou etre temporairement indisponibles.
    for step, label, fn in [
        ("4/10", "Pistes cyclables", export_cycling_to_postgres),
        ("5/10", "Stations métro et RER (IDFM)", export_metro_to_postgres),
        ("6/10", "Données diplômes INSEE", export_diplomes_to_postgres),
    ]:
        print(f"\n[{step}] {label}")
        try:
            fn()
        except Exception as e:
            print(f"  ⚠ Echec ({type(e).__name__}: {e})")
            print(f"  → Le dashboard sera créé sans ces données.")

    client = httpx.Client(base_url=METABASE_URL, timeout=30)

    print("\n[7/10] Connexion à Metabase")
    wait_for_metabase(client)

    print("\n[8/10] Configuration admin")
    session_id = setup_admin(client)
    client.headers["X-Metabase-Session"] = session_id

    # Si l'instance a ete setup avec un ancien site-name (ex: "France Aujourd'hui"),
    # on le force a la valeur courante. PUT idempotent.
    try:
        client.put("/api/setting/site-name", json={"value": "Panorama Île-de-France"})
    except Exception:
        pass

    print("\n[9/10] Base de données PostgreSQL")
    db_id = add_postgres_database(client)

    print("\n[10/10] Cartes GeoJSON + dashboard")
    register_geojson_maps(client)
    dash_id = create_tabbed_dashboard(client, db_id)

    print(f"\n{'═' * 55}")
    print(f"  Metabase : {METABASE_URL}")
    print(f"  Dashboard : {METABASE_URL}/dashboard/{dash_id}")
    # On n'affiche plus le mot de passe en clair (prod).
    print(f"  Admin : {ADMIN_EMAIL} (mdp dans .env / MB_ADMIN_PASSWORD)")
    print("  3 onglets : Paris | Petite couronne | Île-de-France")
    print(f"{'═' * 55}\n")

    client.close()


if __name__ == "__main__":
    main()
