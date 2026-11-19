"""Delta table housekeeping.

OPTIMIZE compacts many small files into fewer big ones (every batch + MERGE
leaves small files behind). ZORDER BY co-locates rows with the same key in the
same files, so queries filtering on that key skip most files.
"""

from __future__ import annotations

import logging

from lakehouse.tables import TableStore

log = logging.getLogger(__name__)

# table -> column most queries filter or join on
ZORDER = {
    "silver_patients": "patient_key",
    "silver_encounters": "patient_key",
    "silver_conditions": "patient_key",
    "silver_medications": "patient_key",
}


def run(store: TableStore) -> None:
    for table, col in ZORDER.items():
        if not store.exists(table):
            continue
        result = store.spark.sql(f"OPTIMIZE {store.sql_ref(table)} ZORDER BY ({col})")
        metrics = result.select("metrics.numFilesAdded", "metrics.numFilesRemoved").first()
        log.info("optimize: %-20s files removed=%s added=%s", table, metrics[1], metrics[0])
    # VACUUM (deleting files no longer referenced) is left at Delta's default
    # 7-day retention on purpose: it is what keeps time travel working.
