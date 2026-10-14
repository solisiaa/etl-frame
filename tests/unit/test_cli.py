"""Unit tests for the Typer CLI (no live services, tiny csv->csv pipelines)."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from typer.testing import CliRunner

from etl.cli import app

runner = CliRunner()


def _write_pipe(path: Path, src: Path, out: Path, extra_source: str = "") -> Path:
    """Write a minimal csv->csv pipeline YAML."""
    cfg = path / "pipe.yaml"
    cfg.write_text(
        f"""
name: cli_e2e
sources:
  - {{name: s, type: csv, params: {{path: {src}}}}}
{extra_source}transformers:
  - {{type: clean, params: {{dedup_on: [order_id]}}}}
sinks:
  - {{name: o, type: csv, params: {{path: {out}, mode: overwrite}}}}
""",
        encoding="utf-8",
    )
    return cfg


def test_validate_ok(tmp_path: Path) -> None:
    """validate exits 0 on a good config."""
    src = tmp_path / "in.csv"
    pd.DataFrame({"a": [1]}).to_csv(src, index=False)
    cfg = _write_pipe(tmp_path, src, tmp_path / "out.csv")
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 0


def test_validate_missing_file() -> None:
    """validate exits 1 when the YAML does not exist."""
    result = runner.invoke(app, ["validate", "nope/missing.yaml"])
    assert result.exit_code == 1


def test_validate_unknown_type(tmp_path: Path) -> None:
    """validate exits 1 on an unregistered plugin type."""
    cfg = tmp_path / "bad.yaml"
    cfg.write_text(
        """
name: bad
sources:
  - {name: s, type: no_such_source_xyz, params: {}}
sinks:
  - {name: o, type: csv, params: {path: out.csv}}
""",
        encoding="utf-8",
    )
    result = runner.invoke(app, ["validate", str(cfg)])
    assert result.exit_code == 1


def test_list_commands() -> None:
    """list-sources/sinks/transformers print the built-ins."""
    for cmd in ("list-sources", "list-sinks", "list-transformers"):
        result = runner.invoke(app, [cmd])
        assert result.exit_code == 0
    assert "csv" in runner.invoke(app, ["list-sources"]).output
    assert "csv" in runner.invoke(app, ["list-sinks"]).output
    assert "clean" in runner.invoke(app, ["list-transformers"]).output


def test_inspect_ok_and_missing(tmp_path: Path) -> None:
    """inspect renders a tree; missing file exits 1."""
    src = tmp_path / "in.csv"
    pd.DataFrame({"a": [1]}).to_csv(src, index=False)
    cfg = _write_pipe(tmp_path, src, tmp_path / "out.csv")
    assert runner.invoke(app, ["inspect", str(cfg)]).exit_code == 0
    assert runner.invoke(app, ["inspect", "nope/missing.yaml"]).exit_code == 1


def test_run_ok_and_dry_run(tmp_path: Path) -> None:
    """run executes csv->csv; --dry-run validates without writing."""
    src = tmp_path / "in.csv"
    pd.DataFrame(
        {"order_id": [1, 2, 2], "customer_id": [1, 2, 2], "amount": [5.0, 6.0, 6.0]}
    ).to_csv(src, index=False)
    out = tmp_path / "out.csv"
    cfg = _write_pipe(tmp_path, src, out)

    dry = runner.invoke(app, ["run", str(cfg), "--dry-run"])
    assert dry.exit_code == 0
    assert not out.exists()

    real = runner.invoke(app, ["run", str(cfg)])
    assert real.exit_code == 0
    assert out.exists()
    assert len(pd.read_csv(out)) == 2


def test_run_missing_config() -> None:
    """run exits 1 on validation error."""
    assert runner.invoke(app, ["run", "nope/missing.yaml"]).exit_code == 1


def test_run_all_sources_fail_exits_2(tmp_path: Path) -> None:
    """run exits 2 when every source fails."""
    out = tmp_path / "out.csv"
    cfg = tmp_path / "pipe.yaml"
    cfg.write_text(
        f"""
name: doomed
sources:
  - {{name: bad, type: csv, params: {{path: {tmp_path}/missing.csv}}}}
sinks:
  - {{name: o, type: csv, params: {{path: {out}}}}}
""",
        encoding="utf-8",
    )
    result = runner.invoke(app, ["run", str(cfg)])
    assert result.exit_code == 2


def test_run_all_empty_dir(tmp_path: Path) -> None:
    """run-all exits 1 when the directory has no pipelines."""
    assert runner.invoke(app, ["run-all", str(tmp_path)]).exit_code == 1


def test_run_all_sequential(tmp_path: Path) -> None:
    """run-all executes every YAML in a directory."""
    for i in (1, 2):
        sub = tmp_path / f"p{i}"
        sub.mkdir()
        src = sub / "in.csv"
        pd.DataFrame({"order_id": [i], "v": [i]}).to_csv(src, index=False)
        # run-all globs only the top directory: put yamls at top level.
        cfg = tmp_path / f"p{i}.yaml"
        cfg.write_text(
            f"""
name: p{i}
sources:
  - {{name: s, type: csv, params: {{path: {src}}}}}
sinks:
  - {{name: o, type: csv, params: {{path: {sub}/out.csv}}}}
""",
            encoding="utf-8",
        )
    result = runner.invoke(app, ["run-all", str(tmp_path)])
    assert result.exit_code == 0
    assert (tmp_path / "p1" / "out.csv").exists()
    assert (tmp_path / "p2" / "out.csv").exists()
