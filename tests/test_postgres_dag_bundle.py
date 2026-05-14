from __future__ import annotations

import zipfile
from io import BytesIO
from pathlib import Path

import pytest

from airflow_postgres_dag_bundle.bundle import DagCodeRow, PostgresDagBundle


class FakePostgresDagBundle(PostgresDagBundle):
    def __init__(
        self,
        *,
        latest_version: str | None,
        rows_by_version: dict[str, list[DagCodeRow]],
        root: Path,
    ) -> None:
        super().__init__(name="project_a")
        self.base_dir = root / "project_a"
        self.versions_dir = self.base_dir / "versions"
        self.tracking_path = self.base_dir / "tracking"
        self.repo_path = self.tracking_path
        self.latest_version = latest_version
        self.rows_by_version = rows_by_version
        self.installed_versions: list[str] = []

    def _get_latest_version(self) -> str | None:
        return self.latest_version

    def _get_source_rows(self, version: str):
        return self.rows_by_version.get(version, [])

    def _install_version(self, version: str, target_path: Path) -> None:
        self.installed_versions.append(version)
        super()._install_version(version, target_path)


def zip_payload(files: dict[str, bytes]) -> bytes:
    buffer = BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        for name, content in files.items():
            archive.writestr(name, content)
    return buffer.getvalue()


def test_refresh_installs_latest_project_version(tmp_path: Path) -> None:
    bundle = FakePostgresDagBundle(
        latest_version="abc123",
        rows_by_version={
            "abc123": [
                DagCodeRow(zip_payload({"dag_1/dag.py": b"print('dag 1')"})),
                DagCodeRow(zip_payload({"dag_2/dag.py": b"print('dag 2')"})),
            ]
        },
        root=tmp_path,
    )

    bundle.refresh()

    assert (bundle.path / "dag_1" / "dag.py").read_bytes() == b"print('dag 1')"
    assert (bundle.path / "dag_2" / "dag.py").read_bytes() == b"print('dag 2')"
    assert (bundle.path / ".bundle-version").read_text(encoding="utf-8") == "abc123"


def test_refresh_skips_when_installed_version_matches(tmp_path: Path) -> None:
    bundle = FakePostgresDagBundle(latest_version="abc123", rows_by_version={}, root=tmp_path)
    bundle.path.mkdir(parents=True)
    (bundle.path / ".bundle-version").write_text("abc123", encoding="utf-8")

    bundle.refresh()

    assert bundle.installed_versions == []


def test_refresh_replaces_old_version(tmp_path: Path) -> None:
    bundle = FakePostgresDagBundle(
        latest_version="new",
        rows_by_version={"new": [DagCodeRow(zip_payload({"dag_1/new.py": b"new"}))]},
        root=tmp_path,
    )
    bundle.path.mkdir(parents=True)
    (bundle.path / ".bundle-version").write_text("old", encoding="utf-8")
    (bundle.path / "dag_1").mkdir()
    (bundle.path / "dag_1" / "old.py").write_text("old", encoding="utf-8")

    bundle.refresh()

    assert not (bundle.path / "dag_1" / "old.py").exists()
    assert (bundle.path / "dag_1" / "new.py").read_bytes() == b"new"


def test_extract_zip_rejects_path_traversal(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="Unsafe path"):
        PostgresDagBundle._extract_zip(zip_payload({"../outside.py": b"bad"}), tmp_path)


def test_versioned_bundle_reuses_existing_matching_version(tmp_path: Path) -> None:
    bundle = FakePostgresDagBundle(latest_version=None, rows_by_version={}, root=tmp_path)
    bundle.version = "abc123"
    bundle.repo_path = bundle.versions_dir / "abc123"
    bundle.path.mkdir(parents=True)
    (bundle.path / ".bundle-version").write_text("abc123", encoding="utf-8")

    bundle.initialize()

    assert bundle.installed_versions == []


def test_versioned_bundle_refuses_to_replace_mismatched_existing_version(tmp_path: Path) -> None:
    bundle = FakePostgresDagBundle(latest_version=None, rows_by_version={}, root=tmp_path)
    bundle.version = "abc123"
    bundle.repo_path = bundle.versions_dir / "abc123"
    bundle.path.mkdir(parents=True)
    (bundle.path / ".bundle-version").write_text("different", encoding="utf-8")

    with pytest.raises(RuntimeError, match="Refusing to replace existing versioned DAG bundle path"):
        bundle.initialize()
