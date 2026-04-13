"""Point d'entrée CLI pour l'ingestion des données."""

import sys

from rich.console import Console

from .config import ALL_V1_DATASETS, ALL_V2_DATASETS
from .download import ingest_dataset
from .prepare import load_all

console = Console()


def main() -> None:
    """Ingestion complète : téléchargement + chargement DuckDB."""
    force = "--force" in sys.argv
    v2 = "--v2" in sys.argv

    console.print("[bold blue]═══ Panorama Ile-de-France — Ingestion ═══[/bold blue]\n")

    datasets = ALL_V1_DATASETS[:]
    if v2:
        datasets += ALL_V2_DATASETS
        console.print("[dim]Mode V2 : inclusion des datasets IRIS[/dim]\n")

    # Phase 1 : téléchargement et extraction
    console.print("[bold]Phase 1 : Téléchargement des datasets[/bold]")
    for ds in datasets:
        try:
            ingest_dataset(ds, force=force)
        except Exception as e:
            console.print(f"  [red]Erreur {ds.name} :[/red] {e}")
            console.print(f"  [dim]URL : {ds.url}[/dim]")
            continue

    # Phase 2 : chargement dans DuckDB
    console.print("\n[bold]Phase 2 : Chargement dans DuckDB[/bold]")
    try:
        load_all()
    except Exception as e:
        console.print(f"[red]Erreur lors du chargement DuckDB :[/red] {e}")
        sys.exit(1)

    console.print("\n[bold green]Ingestion terminée.[/bold green]")


if __name__ == "__main__":
    main()
