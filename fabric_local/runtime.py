"""A local stand-in for the Fabric Spark runtime.

* One SparkSession with Delta Lake (pip installed ``delta-spark``, no Docker).
* The JVM working directory is the lakehouse root, so relative paths such as
  ``Files/landing/...`` and ``Tables/silver_orders`` resolve exactly as they do
  against the default lakehouse attached to a Fabric notebook.
* Every Delta folder under ``Tables/`` is registered in the session catalog, which
  mimics the lakehouse table discovery that makes ``SELECT * FROM silver_orders``
  work in a Fabric notebook.
* ``notebookutils`` exposes the handful of calls the notebooks use.
"""
from __future__ import annotations

import os
import shutil
from pathlib import Path

from . import paths
from .notebook import NotebookExit

_SPARK = None


def get_spark(lakehouse: Path | None = None):
    """Create (once per process) a Delta enabled local SparkSession rooted at the lakehouse."""
    global _SPARK
    if _SPARK is not None:
        return _SPARK
    lakehouse = Path(lakehouse or paths.lakehouse_root())
    (lakehouse / "Files").mkdir(parents=True, exist_ok=True)
    (lakehouse / "Tables").mkdir(parents=True, exist_ok=True)
    os.chdir(lakehouse)  # the JVM inherits this as user.dir: relative paths hit the lakehouse

    from delta import configure_spark_with_delta_pip
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master(os.environ.get("SPARK_MASTER", "local[2]"))
        .appName("fabric-lakehouse-lab")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .config("spark.sql.warehouse.dir", str(lakehouse / "Tables"))
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        .config("spark.databricks.delta.snapshotPartitions", "2")
    )
    _SPARK = configure_spark_with_delta_pip(builder).getOrCreate()
    _SPARK.sparkContext.setLogLevel("ERROR")
    return _SPARK


def register_lakehouse_tables(spark, tables_dir: Path | None = None) -> list[str]:
    """Expose every Delta table under ``Tables/`` to Spark SQL by name."""
    # The set is kept per session instead of asking spark.catalog.listTables(), which fails
    # under the Delta session catalog once temporary views exist.
    tables_dir = Path(tables_dir or paths.tables_dir())
    known = _REGISTERED.setdefault(id(spark), set())
    registered = []
    if not tables_dir.is_dir():
        return registered
    for p in sorted(tables_dir.iterdir()):
        if p.is_dir() and paths.is_delta_table(p):
            if p.name not in known:
                spark.sql(f"CREATE TABLE IF NOT EXISTS `{p.name}` USING DELTA LOCATION '{p.resolve().as_posix()}'")
                known.add(p.name)
            registered.append(p.name)
    return registered


_REGISTERED: dict[int, set[str]] = {}


class _NotebookApi:
    def exit(self, value) -> None:
        raise NotebookExit("" if value is None else str(value))


class _FsApi:
    """Subset of ``notebookutils.fs`` mapped onto the local lakehouse folder."""

    def __init__(self, root: Path):
        self.root = root

    def _p(self, path: str) -> Path:
        return self.root / path.lstrip("/")

    def exists(self, path: str) -> bool:
        return self._p(path).exists()

    def mkdirs(self, path: str) -> bool:
        self._p(path).mkdir(parents=True, exist_ok=True)
        return True

    def ls(self, path: str) -> list[str]:
        return sorted(str(p.relative_to(self.root).as_posix()) for p in self._p(path).iterdir())

    def rm(self, path: str, recurse: bool = False) -> bool:
        p = self._p(path)
        if p.is_dir():
            if not recurse:
                raise IsADirectoryError(path)
            shutil.rmtree(p)
        elif p.exists():
            p.unlink()
        return True


class NotebookUtils:
    """``notebookutils`` (formerly ``mssparkutils``) as far as this lab needs it."""

    def __init__(self, lakehouse: Path | None = None):
        root = Path(lakehouse or paths.lakehouse_root())
        self.notebook = _NotebookApi()
        self.fs = _FsApi(root)
        self.runtime = {"lakehouse": paths.LAKEHOUSE, "workspace": paths.WORKSPACE, "local": True}
