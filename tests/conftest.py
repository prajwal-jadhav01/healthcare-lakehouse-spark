import csv
from pathlib import Path

import pytest

from lakehouse.config import Config
from lakehouse.spark import get_spark
from lakehouse.tables import TableStore

PATIENT_COLS = ["Id", "BIRTHDATE", "DEATHDATE", "SSN", "DRIVERS", "PASSPORT", "PREFIX", "FIRST", "MIDDLE",
                "LAST", "SUFFIX", "MAIDEN", "MARITAL", "RACE", "ETHNICITY", "GENDER", "BIRTHPLACE", "ADDRESS",
                "CITY", "STATE", "COUNTY", "FIPS", "ZIP", "LAT", "LON", "HEALTHCARE_EXPENSES",
                "HEALTHCARE_COVERAGE", "INCOME"]
ENCOUNTER_COLS = ["Id", "START", "STOP", "PATIENT", "ORGANIZATION", "PROVIDER", "PAYER", "ENCOUNTERCLASS",
                  "CODE", "DESCRIPTION", "BASE_ENCOUNTER_COST", "TOTAL_CLAIM_COST", "PAYER_COVERAGE",
                  "REASONCODE", "REASONDESCRIPTION"]
CONDITION_COLS = ["START", "STOP", "PATIENT", "ENCOUNTER", "SYSTEM", "CODE", "DESCRIPTION"]
MEDICATION_COLS = ["START", "STOP", "PATIENT", "PAYER", "ENCOUNTER", "CODE", "DESCRIPTION", "BASE_COST",
                   "PAYER_COVERAGE", "DISPENSES", "TOTALCOST", "REASONCODE", "REASONDESCRIPTION"]
COLUMNS = {"patients": PATIENT_COLS, "encounters": ENCOUNTER_COLS,
           "conditions": CONDITION_COLS, "medications": MEDICATION_COLS}


def patient(id_, **kw):
    row = dict.fromkeys(PATIENT_COLS, "")
    row.update(Id=id_, BIRTHDATE="1980-05-01", SSN="999-11-2222", FIRST="Jane", LAST="Doe", MARITAL="S",
               RACE="white", ETHNICITY="nonhispanic", GENDER="F", ADDRESS="1 Main St", CITY="Boston",
               STATE="Massachusetts", COUNTY="Suffolk County", ZIP="02108", HEALTHCARE_EXPENSES="1000.50",
               HEALTHCARE_COVERAGE="200.00", INCOME="50000")
    row.update(kw)
    return row


def encounter(id_, patient_id, start, stop, cls="ambulatory", cost="100.00", **kw):
    row = dict.fromkeys(ENCOUNTER_COLS, "")
    row.update(Id=id_, START=start, STOP=stop, PATIENT=patient_id, ENCOUNTERCLASS=cls, CODE="1",
               DESCRIPTION="visit", BASE_ENCOUNTER_COST="50.00", TOTAL_CLAIM_COST=cost, PAYER_COVERAGE="80.00")
    row.update(kw)
    return row


@pytest.fixture(scope="session")
def spark():
    s = get_spark(Config(spark_master="local[2]"), "lakehouse-tests")
    yield s
    s.stop()


@pytest.fixture
def lake(spark, tmp_path):
    """A fresh, empty lakehouse in a temp folder, plus a helper to drop CSV batches into landing."""
    cfg = Config(landing_root=str(tmp_path / "landing"), lake_root=str(tmp_path / "lake"),
                 phi_hash_salt="test-salt", max_reject_ratio=0.5)
    store = TableStore(spark, cfg)

    def land(batch_id, entity, rows):
        folder = Path(cfg.landing_root) / batch_id
        folder.mkdir(parents=True, exist_ok=True)
        with open(folder / f"{entity}.csv", "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=COLUMNS[entity])
            w.writeheader()
            w.writerows(rows)

    store.land = land
    return store
