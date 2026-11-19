import pytest
from pyspark.sql import functions as F

from lakehouse import bronze, silver
from lakehouse.quality import DataQualityError
from tests.conftest import encounter, patient


def load(lake, batch_id):
    bronze.run(lake, batch_id)
    silver.run(lake, batch_id)


def test_patients_are_deidentified(lake):
    lake.land("b1", "patients", [patient("p1", ZIP="02108", BIRTHDATE="1980-05-01")])
    load(lake, "b1")

    row = lake.read("silver_patients").first().asDict()
    for phi in ["Id", "SSN", "FIRST", "LAST", "ADDRESS", "LAT", "LON", "BIRTHDATE"]:
        assert phi not in row
    assert row["patient_key"] != "p1" and len(row["patient_key"]) == 64
    assert row["zip3"] == "021"
    assert row["birth_year"] == 1980
    assert row["marital_status"] == "single"


def test_patient_aged_90_plus_has_no_birth_year(lake):
    lake.land("b1", "patients", [patient("old", BIRTHDATE="1920-01-01")])
    load(lake, "b1")
    assert lake.read("silver_patients").first()["birth_year"] is None


def test_scd2_keeps_history_when_a_patient_changes(lake):
    lake.land("b1", "patients", [patient("p1"), patient("p2")])
    load(lake, "b1")
    lake.land("b2", "patients", [patient("p1", CITY="Worcester", MARITAL="M"), patient("p2")])
    load(lake, "b2")

    df = lake.read("silver_patients")
    assert df.count() == 3                       # p1 twice, p2 once (unchanged)
    assert df.where("is_current").count() == 2

    p1 = df.where(F.col("city").isin("Boston", "Worcester")).orderBy("effective_from").collect()
    old, new = [r for r in p1 if r["patient_key"] == p1[-1]["patient_key"]][-2:]
    assert (old["city"], old["is_current"]) == ("Boston", False)
    assert (new["city"], new["is_current"], new["marital_status"]) == ("Worcester", True, "married")
    assert old["effective_to"] == new["effective_from"]


def test_rerunning_a_batch_changes_nothing(lake):
    lake.land("b1", "patients", [patient("p1")])
    lake.land("b2", "patients", [patient("p1", CITY="Lowell")])
    load(lake, "b1")
    load(lake, "b2")
    versions_before = lake.delta("silver_patients").history().count()
    load(lake, "b2")
    assert lake.read("silver_patients").count() == 2
    assert lake.delta("silver_patients").history().count() == versions_before   # no new commit


def test_bad_rows_go_to_quarantine(lake):
    lake.land("b1", "encounters", [
        encounter("e1", "p1", "2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z"),
        encounter("e4", "p1", "2024-02-01T10:00:00Z", "2024-02-01T11:00:00Z"),
        encounter("e5", "p1", "2024-03-01T10:00:00Z", "2024-03-01T11:00:00Z"),
        encounter("e2", "p1", "2024-01-02T10:00:00Z", "2024-01-01T11:00:00Z"),   # stops before it starts
        encounter("e3", "p1", "2024-01-03T10:00:00Z", "2024-01-03T11:00:00Z", cost="-5"),
    ])
    load(lake, "b1")

    good = sorted(r.encounter_id for r in lake.read("silver_encounters").collect())
    assert good == ["e1", "e4", "e5"]
    rules = {tuple(r.failed_rules) for r in lake.read("silver_quarantine").collect()}
    assert rules == {("stop_after_start",), ("cost_not_negative",)}


def test_too_many_bad_rows_fails_the_job(lake):
    lake.land("b1", "patients", [patient("p1", GENDER="X"), patient("p2", GENDER="X"), patient("p3")])
    bronze.run(lake, "b1")
    with pytest.raises(DataQualityError):
        silver.run(lake, "b1")   # 2/3 bad > 50% limit set in the fixture


def test_encounter_correction_is_merged_not_duplicated(lake):
    lake.land("b1", "encounters", [encounter("e1", "p1", "2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z")])
    load(lake, "b1")
    lake.land("b2", "encounters", [encounter("e1", "p1", "2024-01-01T10:00:00Z", "2024-01-01T11:00:00Z",
                                             cost="150.00")])
    load(lake, "b2")

    rows = lake.read("silver_encounters").collect()
    assert len(rows) == 1
    assert float(rows[0].total_claim_cost) == 150.0
