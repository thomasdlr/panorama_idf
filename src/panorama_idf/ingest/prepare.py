"""Préparation des données brutes : normalisation minimale avant chargement dbt/DuckDB.

Le principe : on fait le minimum ici (conversion de format, filtrage IDF grossier),
la vraie logique de nettoyage est dans dbt staging.
"""

from pathlib import Path

import duckdb
from rich.console import Console

from .config import DVF_ANNEES, IDF_DEPARTEMENTS, IDF_REGION, PROCESSED_DIR, RAW_DIR

console = Console()

DB_PATH = PROCESSED_DIR.parent / "panorama_idf.duckdb"


def get_connection() -> duckdb.DuckDBPyConnection:
    """Connexion DuckDB vers le warehouse local."""
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    return duckdb.connect(str(DB_PATH))


def load_cog_communes(con: duckdb.DuckDBPyConnection) -> None:
    """Charge le COG (Code Officiel Géographique) des communes."""
    src = RAW_DIR / "cog_communes_2024.csv"
    if not src.exists():
        console.print("[yellow]COG communes non trouvé, skip[/yellow]")
        return

    console.print("[bold]Chargement COG communes…[/bold]")
    con.execute("DROP TABLE IF EXISTS raw_cog_communes")
    con.execute(
        f"""
        CREATE TABLE raw_cog_communes AS
        SELECT *
        FROM read_csv('{src}', auto_detect=true, header=true, all_varchar=true)
    """
    )
    count = con.execute("SELECT count(*) FROM raw_cog_communes").fetchone()[0]
    console.print(f"  [green]{count:,} communes chargées[/green]")


def load_stats_dvf(con: duckdb.DuckDBPyConnection) -> None:
    """Charge les statistiques DVF agrégées."""
    src = RAW_DIR / "stats_dvf.csv"
    if not src.exists():
        console.print("[yellow]Stats DVF non trouvé, skip[/yellow]")
        return

    console.print("[bold]Chargement statistiques DVF…[/bold]")
    con.execute("DROP TABLE IF EXISTS raw_stats_dvf")
    con.execute(
        f"""
        CREATE TABLE raw_stats_dvf AS
        SELECT *
        FROM read_csv('{src}', auto_detect=true, header=true, all_varchar=true,
                      quote='"')
    """
    )
    count = con.execute("SELECT count(*) FROM raw_stats_dvf").fetchone()[0]
    console.print(f"  [green]{count:,} lignes chargées[/green]")


def load_dvf_plus(con: duckdb.DuckDBPyConnection) -> None:
    """Charge les fichiers DVF géolocalisés IDF dans une seule table."""
    files = []
    for annee in DVF_ANNEES:
        for dep in IDF_DEPARTEMENTS:
            csv_path = RAW_DIR / f"dvf_plus_{dep}_{annee}.csv"
            gz_path = RAW_DIR / f"dvf_plus_{dep}_{annee}.csv.gz"
            if csv_path.exists():
                files.append(str(csv_path))
            elif gz_path.exists():
                files.append(str(gz_path))

    if not files:
        console.print("[yellow]Aucun fichier DVF+ trouvé, skip[/yellow]")
        return

    console.print(f"[bold]Chargement DVF ({len(files)} fichiers)…[/bold]")
    con.execute("DROP TABLE IF EXISTS raw_dvf_plus")

    files_str = ", ".join(f"'{f}'" for f in files)
    con.execute(
        f"""
        CREATE TABLE raw_dvf_plus AS
        SELECT *
        FROM read_csv([{files_str}], auto_detect=true, header=true, all_varchar=true,
                      union_by_name=true, ignore_errors=true)
    """
    )
    count = con.execute("SELECT count(*) FROM raw_dvf_plus").fetchone()[0]
    console.print(f"  [green]{count:,} mutations chargées[/green]")


def load_filosofi_communes(con: duckdb.DuckDBPyConnection) -> None:
    """Charge Filosofi revenus communaux."""
    src = RAW_DIR / "FILO2021_DEC_COM.csv"
    if not src.exists():
        console.print("[yellow]Filosofi communes non trouvé, skip[/yellow]")
        return

    console.print("[bold]Chargement Filosofi communes…[/bold]")
    con.execute("DROP TABLE IF EXISTS raw_filosofi_communes")
    con.execute(
        f"""
        CREATE TABLE raw_filosofi_communes AS
        SELECT *
        FROM read_csv('{src}', auto_detect=true, header=true, all_varchar=true,
                      delim=';')
    """
    )
    count = con.execute("SELECT count(*) FROM raw_filosofi_communes").fetchone()[0]
    console.print(f"  [green]{count:,} communes chargées[/green]")


def load_population_communes(con: duckdb.DuckDBPyConnection) -> None:
    """Charge la population communale historique."""
    xlsx = RAW_DIR / "base-pop-historiques-1876-2023.xlsx"
    if not xlsx.exists():
        console.print("[yellow]Population communes non trouvé, skip[/yellow]")
        return

    console.print("[bold]Chargement population communale…[/bold]")
    con.execute("INSTALL excel; LOAD excel;")
    con.execute("DROP TABLE IF EXISTS raw_population_communes")
    con.execute(
        f"""
        CREATE TABLE raw_population_communes AS
        SELECT *
        FROM read_xlsx('{xlsx}', header=true, range='A6:AZ40000', all_varchar=true)
    """
    )
    count = con.execute("SELECT count(*) FROM raw_population_communes").fetchone()[0]
    console.print(f"  [green]{count:,} lignes chargées[/green]")


def load_population_age(con: duckdb.DuckDBPyConnection) -> None:
    """Charge la population par âge quinquennal."""
    src = RAW_DIR / "TD_POP1B_2021.csv"
    if not src.exists():
        console.print("[yellow]Population âge non trouvé, skip[/yellow]")
        return

    console.print("[bold]Chargement population par âge…[/bold]")
    con.execute("DROP TABLE IF EXISTS raw_population_age")
    con.execute(
        f"""
        CREATE TABLE raw_population_age AS
        SELECT *
        FROM read_csv('{src}', auto_detect=true, header=true, all_varchar=true,
                      delim=';')
    """
    )
    count = con.execute("SELECT count(*) FROM raw_population_age").fetchone()[0]
    console.print(f"  [green]{count:,} lignes chargées[/green]")


def load_loyers_communes(con: duckdb.DuckDBPyConnection) -> None:
    """Charge la carte des loyers par commune."""
    src = RAW_DIR / "loyers_communes_2025.csv"
    if not src.exists():
        console.print("[yellow]Loyers communes non trouvé, skip[/yellow]")
        return

    console.print("[bold]Chargement loyers communes…[/bold]")
    # Convert from latin1 to utf-8 if needed
    utf8_path = src.with_suffix(".utf8.csv")
    if not utf8_path.exists():
        with (
            open(src, encoding="latin-1") as f_in,
            open(utf8_path, "w", encoding="utf-8") as f_out,
        ):
            f_out.write(f_in.read())
    con.execute("DROP TABLE IF EXISTS raw_loyers_communes")
    con.execute(
        f"""
        CREATE TABLE raw_loyers_communes AS
        SELECT *
        FROM read_csv('{utf8_path}', header=true, all_varchar=true,
                      delim=';', quote='"')
    """
    )
    count = con.execute("SELECT count(*) FROM raw_loyers_communes").fetchone()[0]
    console.print(f"  [green]{count:,} lignes chargées[/green]")


def load_delinquance_communes(con: duckdb.DuckDBPyConnection) -> None:
    """Charge la délinquance par commune."""
    csv_path = RAW_DIR / "delinquance_communes.csv"
    gz_path = RAW_DIR / "delinquance_communes.csv.gz"
    src = csv_path if csv_path.exists() else gz_path
    if not src.exists():
        console.print("[yellow]Délinquance communes non trouvé, skip[/yellow]")
        return

    console.print("[bold]Chargement délinquance communes…[/bold]")
    con.execute("DROP TABLE IF EXISTS raw_delinquance_communes")
    con.execute(
        f"""
        CREATE TABLE raw_delinquance_communes AS
        SELECT *
        FROM read_csv('{src}', auto_detect=true, header=true, all_varchar=true,
                      delim=';', quote='"', null_padding=true)
    """
    )
    count = con.execute("SELECT count(*) FROM raw_delinquance_communes").fetchone()[0]
    console.print(f"  [green]{count:,} lignes chargées[/green]")


def load_all() -> None:
    """Charge toutes les données brutes dans DuckDB."""
    console.print(
        "\n[bold blue]═══ Chargement des données dans DuckDB ═══[/bold blue]\n"
    )
    con = get_connection()

    load_cog_communes(con)
    load_stats_dvf(con)
    load_dvf_plus(con)
    load_filosofi_communes(con)
    load_population_communes(con)
    load_population_age(con)
    load_loyers_communes(con)
    load_delinquance_communes(con)

    # Liste les tables créées
    tables = con.execute("SHOW TABLES").fetchall()
    console.print(
        f"\n[bold green]Tables dans DuckDB :[/bold green] {[t[0] for t in tables]}"
    )
    con.close()
