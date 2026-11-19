"""Command-line entry point used by Airflow, Makefile and Databricks.

    python -m lakehouse.jobs.run --layer bronze --batch-id batch_001
    python -m lakehouse.jobs.run --layer all    --batch-id batch_001
"""

from __future__ import annotations

import argparse
import logging
import time

from lakehouse import bronze, gold, maintenance, silver
from lakehouse.config import Config
from lakehouse.spark import get_spark
from lakehouse.tables import TableStore

LAYERS = ["bronze", "silver", "gold", "optimize"]


def run_layer(store: TableStore, layer: str, batch_id: str | None) -> None:
    started = time.time()
    if layer == "bronze":
        bronze.run(store, _need(batch_id, layer))
    elif layer == "silver":
        silver.run(store, _need(batch_id, layer))
    elif layer == "gold":
        gold.run(store)
    elif layer == "optimize":
        maintenance.run(store)
    else:
        raise ValueError(f"unknown layer {layer!r}")
    logging.getLogger("lakehouse").info("%s finished in %.1fs", layer, time.time() - started)


def _need(batch_id: str | None, layer: str) -> str:
    if not batch_id:
        raise SystemExit(f"--batch-id is required for the {layer} layer")
    return batch_id


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--layer", choices=LAYERS + ["all"], required=True)
    parser.add_argument("--batch-id", help="landing folder name, e.g. batch_001")
    args = parser.parse_args(argv)

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
    cfg = Config.from_env()
    store = TableStore(get_spark(cfg), cfg)

    for layer in (LAYERS if args.layer == "all" else [args.layer]):
        run_layer(store, layer, args.batch_id)


if __name__ == "__main__":
    main()
