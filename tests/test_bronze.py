from pyspark.sql import functions as F

from lakehouse import bronze
from tests.conftest import patient


def test_bronze_keeps_raw_strings_and_adds_lineage(lake):
    lake.land("b1", "patients", [patient("p1"), patient("p2")])
    counts = bronze.run(lake, "b1")

    df = lake.read("bronze_patients")
    assert counts["patients"] == 2
    assert counts["encounters"] == 0          # missing file is skipped, not an error
    assert dict(df.dtypes)["INCOME"] == "string"
    assert {"_batch_id", "_source_file", "_ingested_at"} <= set(df.columns)
    assert df.where(F.col("_source_file").endswith("b1/patients.csv")).count() == 2


def test_bronze_rerun_of_same_batch_does_not_duplicate(lake):
    lake.land("b1", "patients", [patient("p1"), patient("p2")])
    lake.land("b2", "patients", [patient("p3")])
    bronze.run(lake, "b1")
    bronze.run(lake, "b2")
    bronze.run(lake, "b1")   # re-run

    df = lake.read("bronze_patients")
    assert df.count() == 3
    assert df.where("_batch_id = 'b1'").count() == 2
