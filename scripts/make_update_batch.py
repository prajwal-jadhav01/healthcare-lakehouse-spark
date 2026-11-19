"""Builds a second landing batch from the first one, to exercise the
"day 2" behaviour of the pipeline:

* ~5% of patients changed address / marital status / income  -> SCD2 new versions
* some exact duplicate patient rows                          -> removed in silver
* ~2% of encounters re-sent with a corrected claim cost       -> MERGE updates
* a few deliberately broken rows                              -> quarantine

    python scripts/make_update_batch.py data/landing/batch_001 data/landing/batch_002
"""

from __future__ import annotations

import csv
import random
import sys
from pathlib import Path

CITIES = [("Boston", "Suffolk County", "02108"), ("Worcester", "Worcester County", "01602"),
          ("Springfield", "Hampden County", "01103"), ("Cambridge", "Middlesex County", "02139"),
          ("Lowell", "Middlesex County", "01852")]


def read(path: Path) -> tuple[list[str], list[dict]]:
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        return reader.fieldnames, list(reader)


def write(path: Path, header: list[str], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def main(src: str, dst: str, seed: int = 7) -> None:
    rnd = random.Random(seed)
    src_dir, dst_dir = Path(src), Path(dst)
    dst_dir.mkdir(parents=True, exist_ok=True)

    # --- patients
    header, patients = read(src_dir / "patients.csv")
    changed = []
    for p in rnd.sample(patients, k=max(1, len(patients) // 20)):
        p = dict(p)
        city, county, zip_code = rnd.choice(CITIES)
        p.update(CITY=city, COUNTY=county, ZIP=zip_code,
                 MARITAL=rnd.choice(["M", "S", "D", "W"]),
                 INCOME=str(int(int(p["INCOME"] or 50000) * rnd.uniform(0.9, 1.3))))
        changed.append(p)
    duplicates = rnd.sample(changed, k=max(1, len(changed) // 5))
    broken = [
        {**changed[0], "Id": ""},                       # id_not_null
        {**changed[1], "Id": "bad-1", "GENDER": "X"},   # gender_valid
        {**changed[2], "Id": "bad-2", "BIRTHDATE": "2999-01-01"},  # birthdate_not_in_future
    ]
    write(dst_dir / "patients.csv", header, changed + duplicates + broken)

    # --- encounters
    header, encounters = read(src_dir / "encounters.csv")
    corrected = []
    for e in rnd.sample(encounters, k=max(1, len(encounters) // 50)):
        e = dict(e)
        e["TOTAL_CLAIM_COST"] = f"{float(e['TOTAL_CLAIM_COST']) * 1.1:.2f}"
        corrected.append(e)
    bad = [
        {**corrected[0], "Id": "bad-enc-1", "STOP": "2000-01-01T00:00:00Z"},  # stop_after_start
        {**corrected[1], "Id": "bad-enc-2", "TOTAL_CLAIM_COST": "-50.00"},     # cost_not_negative
    ]
    write(dst_dir / "encounters.csv", header, corrected + bad)

    # conditions / medications: nothing new today (empty files with headers only)
    for name in ("conditions", "medications"):
        header, _ = read(src_dir / f"{name}.csv")
        write(dst_dir / f"{name}.csv", header, [])

    print(f"{dst_dir}: {len(changed)} changed patients, {len(duplicates)} duplicates, "
          f"{len(broken)} broken patients, {len(corrected)} corrected encounters, {len(bad)} broken encounters")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
