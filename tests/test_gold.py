from datetime import datetime
from decimal import Decimal

from lakehouse import gold


def test_readmission_within_30_days(spark):
    rows = [
        # patient A: discharged Jan 5, back Jan 20 (15 days) -> readmission; then Jun 1 (far later) -> not
        ("e1", "A", "inpatient", datetime(2024, 1, 1), datetime(2024, 1, 5)),
        ("e2", "A", "inpatient", datetime(2024, 1, 20), datetime(2024, 1, 22)),
        ("e3", "A", "inpatient", datetime(2024, 6, 1), datetime(2024, 6, 3)),
        # patient B: one stay, plus an outpatient visit that must be ignored
        ("e4", "B", "inpatient", datetime(2024, 1, 2), datetime(2024, 1, 4)),
        ("e5", "B", "outpatient", datetime(2024, 1, 10), datetime(2024, 1, 10)),
    ]
    enc = spark.createDataFrame(rows, "encounter_id string, patient_key string, encounter_class string, "
                                      "start_ts timestamp, stop_ts timestamp")
    out = {r.discharge_month.isoformat(): r for r in gold.readmissions_30d(enc).collect()}

    assert out["2024-01-01"].discharges == 3
    assert out["2024-01-01"].readmissions_30d == 1
    assert out["2024-01-01"].readmission_rate == round(1 / 3, 4)
    assert out["2024-06-01"].readmissions_30d == 0


def test_condition_cost_adds_encounter_and_medication_cost(spark):
    enc = spark.createDataFrame(
        [("A", "44054006", "Diabetes", Decimal("100.00"), Decimal("80.00")),
         ("B", "44054006", "Diabetes", Decimal("300.00"), Decimal("240.00")),
         ("A", None, None, Decimal("999.00"), Decimal("0.00"))],
        "patient_key string, reason_code string, reason_description string, "
        "total_claim_cost decimal(18,2), payer_coverage decimal(18,2)")
    meds = spark.createDataFrame([("44054006", Decimal("50.00"))], "reason_code string, total_cost decimal(18,2)")

    row = gold.condition_cost(enc, meds).collect()[0]
    assert row.condition == "Diabetes"
    assert row.patients == 2
    assert row.total_cost_of_care == Decimal("450.00")
    assert row.cost_per_patient == Decimal("225.00")
    assert row.payer_coverage_ratio == Decimal("0.8")
