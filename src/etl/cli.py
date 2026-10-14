"""Typer CLI for the ETL framework (Rich-powered output)."""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn
from rich.table import Table
from rich.tree import Tree

# Import for side-effects: registers csv/api/postgres + all sinks/transformers.
import etl.sinks  # noqa: F401
import etl.sources  # noqa: F401
import etl.transformers  # noqa: F401
from etl.config.loader import load_config
from etl.core.context import PipelineContext
from etl.core.exceptions import ConfigError, PipelineError
from etl.core.pipeline import run_pipeline
from etl.core.registry import list_sinks, list_sources, list_transformers

app = typer.Typer(name="etl", help="Extensible ETL framework CLI.", no_args_is_help=True)
console = Console()


def _print_metrics_table(ctx: PipelineContext) -> None:
    table = Table(title="Pipeline metrics", show_lines=False)
    table.add_column("metric", style="cyan")
    table.add_column("stats", style="green")
    for key, stats in ctx.metrics.summary().items():
        table.add_row(
            key, f"count={stats['count']} sum={stats['sum']:.3f} last={stats['last']:.3f}"
        )
    console.print(table)


@app.command()
def run(
    config: Annotated[Path, typer.Argument(help="Path to pipeline YAML.")],
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Validate & plan without writing.")
    ] = False,
    verbose: Annotated[bool, typer.Option("-v", "--verbose")] = False,
) -> None:
    """Run a pipeline end-to-end (exit 0 ok, 1 validation error, 2 runtime error)."""
    import logging

    if verbose:
        logging.getLogger("etl").setLevel(logging.DEBUG)
    try:
        cfg = load_config(config)
    except ConfigError as exc:
        console.print(f"[red]Validation error:[/red] {exc}")
        raise typer.Exit(code=1) from exc

    ctx = PipelineContext(max_concurrency=cfg.execution.max_concurrency, dry_run=dry_run)
    console.print(f"[bold]Running pipeline[/bold] {cfg.name} [dim]({cfg.description})[/dim]")
    with Progress(
        SpinnerColumn(), TextColumn("[progress.description]{task.description}"), console=console
    ) as progress:
        task = progress.add_task("extract → transform → load…", total=None)
        try:
            result = run_pipeline(cfg, ctx)
        except PipelineError as exc:
            progress.stop()
            console.print(f"[red]Pipeline failed:[/red] {exc}")
            raise typer.Exit(code=2) from exc
        except Exception as exc:  # noqa: BLE001 - surface any sink error as exit 2
            progress.stop()
            console.print(f"[red]Execution error:[/red] {exc}")
            raise typer.Exit(code=2) from exc
        progress.update(task, completed=True)

    console.print(
        f"[green]Done[/green] rows_in={result.rows_in} rows_out={result.rows_out} "
        f"duration={result.duration_s:.2f}s "
        f"sources_ok={result.successes} sources_failed={result.failures}"
    )
    if result.errors:
        for e in result.errors:
            console.print(f"[yellow]WARN:[/yellow] {e}")
        raise typer.Exit(code=2)
    _print_metrics_table(ctx)


@app.command()
def validate(config: Annotated[Path, typer.Argument(help="Path to pipeline YAML.")]) -> None:
    """Validate a config without executing (exit 1 on error)."""
    try:
        cfg = load_config(config)
        # Also verify that plugin types exist.
        from etl.core.registry import get_sink, get_source, get_transformer

        for src in cfg.sources:
            get_source(src.type)
        for t in cfg.transformers:
            get_transformer(t.type)
        for snk in cfg.sinks:
            get_sink(snk.type)
    except (ConfigError, Exception) as exc:  # noqa: BLE001
        console.print(f"[red]Invalid:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    console.print(f"[green]Valid[/green] pipeline '{cfg.name}' ✓")


@app.command(name="list-sources")
def list_sources_cmd() -> None:
    """List registered source types."""
    table = Table(title="Sources")
    table.add_column("type", style="cyan")
    for name in list_sources():
        table.add_row(name)
    console.print(table)


@app.command(name="list-sinks")
def list_sinks_cmd() -> None:
    """List registered sink types."""
    table = Table(title="Sinks")
    table.add_column("type", style="cyan")
    for name in list_sinks():
        table.add_row(name)
    console.print(table)


@app.command(name="list-transformers")
def list_transformers_cmd() -> None:
    """List registered transformer types."""
    table = Table(title="Transformers")
    table.add_column("type", style="cyan")
    for name in list_transformers():
        table.add_row(name)
    console.print(table)


@app.command()
def inspect(config: Annotated[Path, typer.Argument(help="Path to pipeline YAML.")]) -> None:
    """Show the pipeline as a Rich tree (sources → transformers → sinks)."""
    try:
        cfg = load_config(config)
    except ConfigError as exc:
        console.print(f"[red]Validation error:[/red] {exc}")
        raise typer.Exit(code=1) from exc
    tree = Tree(f"[bold]{cfg.name}[/bold] [dim]{cfg.description}[/dim]")
    src_branch = tree.add("sources")
    for src_cfg in cfg.sources:
        src_branch.add(f"{src_cfg.name} [cyan]({src_cfg.type})[/cyan] {src_cfg.params}")
    tr_branch = tree.add("transformers")
    for t in cfg.transformers:
        tr_branch.add(f"[magenta]{t.type}[/magenta] {t.params}")
    sink_branch = tree.add("sinks")
    for snk_cfg in cfg.sinks:
        sink_branch.add(f"{snk_cfg.name} [green]({snk_cfg.type})[/green] {snk_cfg.params}")
    tree.add(
        f"execution: parallel={cfg.execution.parallel_sources} "
        f"concurrency={cfg.execution.max_concurrency}"
    )
    console.print(tree)


@app.command(name="run-all")
def run_all(
    directory: Annotated[Path, typer.Argument(help="Directory with *.yaml pipelines.")],
    parallel: Annotated[bool, typer.Option("--parallel/--sequential")] = False,
    dry_run: Annotated[bool, typer.Option("--dry-run")] = False,
) -> None:
    """Run every ``*.yaml``/``*.yml`` pipeline in a directory."""
    import concurrent.futures
    import subprocess
    import sys

    files = sorted([*directory.glob("*.yaml"), *directory.glob("*.yml")])
    if not files:
        console.print(f"[yellow]No pipelines found in {directory}[/yellow]")
        raise typer.Exit(code=1)
    console.print(f"Found {len(files)} pipeline(s)")

    def _run_one(path: Path) -> tuple[str, int]:
        cmd = [sys.executable, "-m", "etl.cli", "run", str(path)]
        if dry_run:
            cmd.append("--dry-run")
        proc = subprocess.run(cmd, capture_output=True, text=True)  # noqa: S603
        console.print(f"--- {path.name} (exit={proc.returncode}) ---")
        if proc.stdout:
            console.print(proc.stdout[-2000:])
        if proc.returncode != 0 and proc.stderr:
            console.print(f"[red]{proc.stderr[-2000:]}[/red]")
        return path.name, proc.returncode

    failed: list[str] = []
    if parallel:
        with concurrent.futures.ThreadPoolExecutor() as pool:
            for name, code in pool.map(_run_one, files):
                if code != 0:
                    failed.append(name)
    else:
        for f in files:
            name, code = _run_one(f)
            if code != 0:
                failed.append(name)
    if failed:
        console.print(f"[red]Failed:[/red] {failed}")
        raise typer.Exit(code=2)
    console.print("[green]All pipelines succeeded[/green]")


if __name__ == "__main__":  # pragma: no cover
    app()
