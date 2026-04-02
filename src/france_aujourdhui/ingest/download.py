"""Téléchargement et extraction des datasets."""

import gzip
import shutil
import zipfile
from pathlib import Path

import httpx
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn

from .config import RAW_DIR, DatasetConfig

console = Console()


def download_file(url: str, dest: Path, force: bool = False) -> Path:
    """Télécharge un fichier si non déjà présent (ou si force=True)."""
    if dest.exists() and not force:
        console.print(f"  [dim]Déjà présent :[/dim] {dest.name}")
        return dest

    dest.parent.mkdir(parents=True, exist_ok=True)

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        console=console,
    ) as progress:
        task = progress.add_task(f"Téléchargement {dest.name}…", total=None)

        with httpx.stream("GET", url, follow_redirects=True, timeout=120.0) as resp:
            resp.raise_for_status()
            with open(dest, "wb") as f:
                for chunk in resp.iter_bytes(chunk_size=65536):
                    f.write(chunk)

        progress.update(task, completed=True)

    console.print(f"  [green]Téléchargé :[/green] {dest.name} ({dest.stat().st_size / 1_048_576:.1f} Mo)")
    return dest


def extract_archive(archive: Path, filenames: list[str], dest_dir: Path) -> list[Path]:
    """Extrait des fichiers spécifiques d'une archive zip."""
    dest_dir.mkdir(parents=True, exist_ok=True)
    extracted = []

    if archive.suffix == ".zip":
        with zipfile.ZipFile(archive) as zf:
            members = zf.namelist()
            for target in filenames:
                # Cherche le fichier dans l'archive (peut être dans un sous-dossier)
                match = [m for m in members if m.endswith(target)]
                if not match:
                    console.print(f"  [yellow]Fichier introuvable dans l'archive :[/yellow] {target}")
                    console.print(f"  [dim]Contenu de l'archive :[/dim] {members[:20]}")
                    continue
                member = match[0]
                dest_path = dest_dir / target
                with zf.open(member) as src, open(dest_path, "wb") as dst:
                    shutil.copyfileobj(src, dst)
                extracted.append(dest_path)
                console.print(f"  [green]Extrait :[/green] {target}")
    else:
        console.print(f"  [yellow]Format d'archive non supporté :[/yellow] {archive.suffix}")

    return extracted


def decompress_gzip(src: Path, dest: Path) -> Path:
    """Décompresse un fichier .gz."""
    if dest.exists():
        console.print(f"  [dim]Déjà décompressé :[/dim] {dest.name}")
        return dest

    with gzip.open(src, "rb") as f_in, open(dest, "wb") as f_out:
        shutil.copyfileobj(f_in, f_out)

    console.print(f"  [green]Décompressé :[/green] {dest.name}")
    return dest


def ingest_dataset(ds: DatasetConfig, force: bool = False) -> list[Path]:
    """Télécharge et prépare un dataset complet."""
    console.print(f"\n[bold]{ds.name}[/bold] — {ds.description}")

    raw_path = RAW_DIR / ds.filename
    download_file(ds.url, raw_path, force=force)

    result_files = []

    if ds.extract:
        result_files = extract_archive(raw_path, ds.extract, RAW_DIR)
    elif raw_path.suffix == ".gz":
        decompressed = RAW_DIR / raw_path.stem
        result_files = [decompress_gzip(raw_path, decompressed)]
    else:
        result_files = [raw_path]

    return result_files
