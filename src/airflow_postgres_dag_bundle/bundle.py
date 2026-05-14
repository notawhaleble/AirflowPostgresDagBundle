from __future__ import annotations

import shutil
import zipfile
from contextlib import contextmanager
from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Iterable, Iterator

try:
    from airflow.dag_processing.bundles.base import BaseDagBundle
except ImportError:  # pragma: no cover - only used by lightweight local tests.
    class BaseDagBundle:  # type: ignore[no-redef]
        supports_versioning = False

        def __init__(
            self,
            *,
            name: str = "test_bundle",
            version: str | None = None,
            refresh_interval: int = 300,
            **_: Any,
        ) -> None:
            self.name = name
            self.version = version
            self.refresh_interval = refresh_interval
            self.base_dir = Path.cwd() / ".airflow-bundles" / name
            self.versions_dir = self.base_dir / "versions"

        def initialize(self) -> None:
            pass

        @contextmanager
        def lock(self) -> Iterator[None]:
            yield


@dataclass(frozen=True)
class DagCodeRow:
    source_code: bytes


class PostgresDagBundle(BaseDagBundle):
    """Airflow 3 DAG bundle backed by the metadata PostgreSQL database."""

    supports_versioning = True

    def __init__(
        self,
        *,
        table_name: str = "versioned_dagcode",
        project_name: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self.table_name = self._validate_table_name(table_name)
        self.project_name = project_name or self.name
        self.tracking_path = self.base_dir / "tracking"
        self.repo_path = self.versions_dir / self.version if self.version else self.tracking_path

    @property
    def path(self) -> Path:
        return self.repo_path

    def initialize(self) -> None:
        if self.version:
            with self.lock():
                self._install_version_if_required(self.version)
        else:
            self.refresh()
        super().initialize()

    def get_current_version(self) -> str | None:
        if self.version:
            return self.version
        return self._get_latest_version()

    def refresh(self) -> None:
        if self.version:
            raise RuntimeError("Refreshing a specific DAG bundle version is not supported")

        latest_version = self._get_latest_version()
        if latest_version is None:
            self.path.mkdir(parents=True, exist_ok=True)
            self._write_installed_version(None, self.path)
            return

        installed_version = self._read_installed_version(self.path)
        if installed_version == latest_version and self.path.exists():
            return

        with self.lock():
            installed_version = self._read_installed_version(self.path)
            if installed_version == latest_version and self.path.exists():
                return
            self._install_version(latest_version, self.path)

    def view_url(self, version: str | None = None) -> str | None:
        return None

    def view_url_template(self) -> str | None:
        return None

    def _install_version_if_required(self, version: str) -> None:
        self._ensure_immutable_version(version, self.path)
        if self.path.exists():
            return
        self._install_version(version, self.path)

    def _install_version(self, version: str, target_path: Path) -> None:
        rows = list(self._get_source_rows(version))
        if not rows:
            raise RuntimeError(
                f"No DAG source rows found for project_name={self.project_name!r}, commit_hash={version!r}"
            )

        target_path.parent.mkdir(parents=True, exist_ok=True)
        with TemporaryDirectory(prefix=f".{target_path.name}.", dir=target_path.parent) as tmp_dir:
            tmp_path = Path(tmp_dir)
            for row in rows:
                self._extract_zip(row.source_code, tmp_path)
            self._write_installed_version(version, tmp_path)
            self._replace_dir(tmp_path, target_path)

    def _ensure_immutable_version(self, version: str, path: Path) -> None:
        if not path.exists():
            return

        installed_version = self._read_installed_version(path)
        if installed_version == version:
            return

        raise RuntimeError(
            f"Refusing to replace existing versioned DAG bundle path {path}: "
            f"expected version {version!r}, found {installed_version!r}"
        )

    def _get_latest_version(self) -> str | None:
        query = self._sql_text(
            f"""
            SELECT commit_hash
            FROM {self.table_name}
            WHERE project_name = :project_name
              AND commit_hash IS NOT NULL
            ORDER BY uploaded DESC
            LIMIT 1
            """
        )
        with self._connect() as conn:
            row = conn.execute(query, {"project_name": self.project_name}).first()
        return row[0] if row else None

    def _get_source_rows(self, version: str) -> Iterable[DagCodeRow]:
        query = self._sql_text(
            f"""
            SELECT source_code
            FROM {self.table_name}
            WHERE project_name = :project_name
              AND commit_hash = :commit_hash
              AND source_code IS NOT NULL
            ORDER BY uploaded ASC
            """
        )
        with self._connect() as conn:
            rows = conn.execute(
                query,
                {"project_name": self.project_name, "commit_hash": version},
            ).all()
        return [DagCodeRow(source_code=self._as_bytes(row[0])) for row in rows]

    @contextmanager
    def _connect(self) -> Iterator[Any]:
        from airflow import settings

        with settings.engine.begin() as conn:
            yield conn

    @staticmethod
    def _sql_text(query: str) -> Any:
        from sqlalchemy import text

        return text(query)

    @staticmethod
    def _validate_table_name(table_name: str) -> str:
        parts = table_name.split(".")
        if not parts or not all(part.isidentifier() for part in parts):
            raise ValueError(f"Invalid table name: {table_name!r}")
        return table_name

    @staticmethod
    def _as_bytes(value: Any) -> bytes:
        if isinstance(value, bytes):
            return value
        if isinstance(value, memoryview):
            return value.tobytes()
        return bytes(value)

    @staticmethod
    def _extract_zip(payload: bytes, target_path: Path) -> None:
        with zipfile.ZipFile(BytesIO(payload)) as archive:
            for member in archive.infolist():
                member_path = target_path / member.filename
                try:
                    member_path.resolve().relative_to(target_path.resolve())
                except ValueError as exc:
                    raise RuntimeError(f"Unsafe path in DAG source zip: {member.filename!r}") from exc

                if member.is_dir():
                    member_path.mkdir(parents=True, exist_ok=True)
                    continue

                member_path.parent.mkdir(parents=True, exist_ok=True)
                with archive.open(member) as source, member_path.open("wb") as destination:
                    shutil.copyfileobj(source, destination)

    @staticmethod
    def _read_installed_version(path: Path) -> str | None:
        version_file = path / ".bundle-version"
        if not version_file.exists():
            return None
        version = version_file.read_text(encoding="utf-8").strip()
        return version or None

    @staticmethod
    def _write_installed_version(version: str | None, path: Path) -> None:
        path.mkdir(parents=True, exist_ok=True)
        (path / ".bundle-version").write_text(version or "", encoding="utf-8")

    @staticmethod
    def _replace_dir(source_path: Path, target_path: Path) -> None:
        backup_path = target_path.with_name(f".{target_path.name}.old")
        if backup_path.exists():
            shutil.rmtree(backup_path)
        if target_path.exists():
            target_path.rename(backup_path)
        source_path.rename(target_path)
        if backup_path.exists():
            shutil.rmtree(backup_path)
