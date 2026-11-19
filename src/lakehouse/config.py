"""Runtime configuration, read from environment variables.

The same code runs in three places, and only this config changes:

* local laptop  -> STORAGE_MODE=path, LAKE_ROOT=./data/lake (plain folders)
* docker compose -> STORAGE_MODE=path, LAKE_ROOT=s3a://lakehouse/lake (MinIO)
* Databricks     -> STORAGE_MODE=catalog, tables live in Unity Catalog
"""

from __future__ import annotations

import os
from dataclasses import dataclass


def _env(name: str, default: str | None = None) -> str | None:
    value = os.environ.get(name)
    return value if value not in (None, "") else default


@dataclass(frozen=True)
class Config:
    # "path": Delta tables stored as folders under lake_root (local disk or S3/MinIO)
    # "catalog": Delta tables registered as <catalog>.<schema>.<name> (Databricks Unity Catalog)
    storage_mode: str = "path"
    landing_root: str = "data/landing"
    lake_root: str = "data/lake"
    catalog: str = "workspace"
    schema: str = "lakehouse"

    # S3-compatible storage (MinIO locally, real S3 later)
    s3_endpoint: str | None = None
    s3_access_key: str | None = None
    s3_secret_key: str | None = None

    # Salt for hashing patient ids. Set a real secret outside of demos.
    phi_hash_salt: str = "change-me-demo-salt"

    # Fail the silver step if more than this share of a batch is quarantined.
    max_reject_ratio: float = 0.10

    spark_master: str = "local[*]"

    @classmethod
    def from_env(cls, **overrides) -> "Config":
        cfg = dict(
            storage_mode=_env("STORAGE_MODE", "path"),
            landing_root=_env("LANDING_ROOT", "data/landing"),
            lake_root=_env("LAKE_ROOT", "data/lake"),
            catalog=_env("CATALOG", "workspace"),
            schema=_env("SCHEMA", "lakehouse"),
            s3_endpoint=_env("S3_ENDPOINT"),
            s3_access_key=_env("S3_ACCESS_KEY"),
            s3_secret_key=_env("S3_SECRET_KEY"),
            phi_hash_salt=_env("PHI_HASH_SALT", "change-me-demo-salt"),
            max_reject_ratio=float(_env("MAX_REJECT_RATIO", "0.10")),
            spark_master=_env("SPARK_MASTER", "local[*]"),
        )
        cfg.update({k: v for k, v in overrides.items() if v is not None})
        if cfg["storage_mode"] not in ("path", "catalog"):
            raise ValueError(f"STORAGE_MODE must be 'path' or 'catalog', got {cfg['storage_mode']!r}")
        return cls(**cfg)
