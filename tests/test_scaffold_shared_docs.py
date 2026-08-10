"""Тесты scaffold и синхронизации общих страниц документации."""

from __future__ import annotations

from pathlib import Path

from prs_connector_core.scaffold import (
    scaffold_deployment,
    scaffold_docs,
    scaffold_readme,
)
from prs_connector_core.sphinx_shared import SHARED_DOCS_DIRNAME, sync_shared_docs


def test_scaffold_deployment_copies_host_network_compose(tmp_path: Path) -> None:
    written = scaffold_deployment(tmp_path)
    names = {path.name for path in written}
    assert "Dockerfile.example" in names
    assert "compose.example.yml" in names
    assert "README.md" in names

    compose = (tmp_path / "deployment" / "docker" / "compose.example.yml").read_text(
        encoding="utf-8"
    )
    assert "network_mode: host" in compose


def test_scaffold_docs_copies_docker_launch(tmp_path: Path) -> None:
    dest = tmp_path / "source"
    written = scaffold_docs(dest)
    names = {path.name for path in written}
    assert "docker_launch.rst" in names
    assert "docker_launch.md" in names

    rst = (dest / "docker_launch.rst").read_text(encoding="utf-8")
    assert "network_mode: host" in rst
    assert "prs_connector_core.sphinx_shared" in rst


def test_scaffold_readme_writes_file(tmp_path: Path) -> None:
    out = scaffold_readme(tmp_path / "README.docker.md")
    assert out.is_file()
    text = out.read_text(encoding="utf-8")
    assert "network_mode: host" in text


def test_sync_shared_docs_for_sphinx(tmp_path: Path) -> None:
    target = sync_shared_docs(tmp_path)
    assert target == tmp_path / SHARED_DOCS_DIRNAME
    assert (target / "docker_launch.rst").is_file()
    assert (target / "docker_launch.md").is_file()
