"""The proving ground compares run records honestly: nothing untested looks green."""

from __future__ import annotations

import json

import pytest
from typer.testing import CliRunner

from ubunye.cli.main import app
from ubunye.proving import (
    Observation,
    compare,
    load_observations,
    observe_record,
    render_markdown,
    skipped,
)
from ubunye.proving.matrix import FAIL, NOT_RECORDED, NOT_RUN, PARTIAL, PASS, UNSUPPORTED

W = "c01"


def record(
    *,
    status="success",
    data="d1",
    schema="s1",
    rows=10,
    code="c1",
    method="rows-v1",
    inputs=True,
    outputs=("out",),
    run_id="r",
):
    return {
        "run_id": run_id,
        "status": status,
        "code_hash": code,
        "config_hash": "cfg-" + run_id,
        "duration_sec": 1.5,
        "engine_version": "0.7.2",
        "backend": "spark",
        "inputs": (
            [
                {
                    "name": "src",
                    "data_hash": "in1",
                    "schema_hash": "si",
                    "row_count": 12,
                    "hash_method": "rows-v1",
                }
            ]
            if inputs
            else []
        ),
        "outputs": [
            {
                "name": n,
                "data_hash": data,
                "schema_hash": schema,
                "row_count": rows,
                "hash_method": method,
            }
            for n in outputs
        ],
    }


def obs(env, **kw):
    return observe_record(record(run_id=env, **kw), workload=W, environment=env)


def verdicts(matrix):
    return {e: r["verdict"] for e, r in matrix["environments"].items()}


def test_equal_records_pass_every_dimension():
    m = compare([obs("spark"), obs("pandas")], reference="spark")
    assert verdicts(m) == {"spark": PASS, "pandas": PASS}
    assert set(m["environments"]["pandas"]["dimensions"].values()) == {PASS}


def test_different_data_fails_data_only():
    m = compare([obs("spark"), obs("pandas", data="d2")], reference="spark")
    dims = m["environments"]["pandas"]["dimensions"]
    assert dims["data"] == FAIL and dims["schema"] == PASS and dims["rows"] == PASS
    assert m["environments"]["pandas"]["verdict"] == FAIL


def test_the_config_hash_is_not_compared():
    # A cloud reads s3:// where a laptop reads a folder: config hashes differ by design.
    m = compare([obs("spark"), obs("glue")], reference="spark")
    assert m["environments"]["glue"]["config_hash"] != m["environments"]["spark"]["config_hash"]
    assert verdicts(m)["glue"] == PASS


def test_different_code_is_a_different_workload():
    m = compare([obs("spark"), obs("pandas", code="c2")], reference="spark")
    assert m["environments"]["pandas"]["dimensions"]["identity"] == FAIL


def test_a_missing_output_fails():
    m = compare(
        [obs("spark", outputs=("a", "b")), obs("pandas", outputs=("a",))], reference="spark"
    )
    assert m["environments"]["pandas"]["dimensions"]["identity"] == FAIL
    assert m["environments"]["pandas"]["dimensions"]["data"] == FAIL


def test_hashes_by_different_methods_are_not_compared():
    m = compare([obs("spark"), obs("old", method="sample-v0")], reference="spark")
    assert m["environments"]["old"]["dimensions"]["data"] == PARTIAL
    assert m["environments"]["old"]["verdict"] == PARTIAL


def test_a_missing_hash_is_not_recorded_not_equal():
    m = compare([obs("spark"), obs("pandas", data=None)], reference="spark")
    assert m["environments"]["pandas"]["dimensions"]["data"] == NOT_RECORDED
    assert m["environments"]["pandas"]["verdict"] == PARTIAL


def test_inputs_recorded_without_a_hash_do_not_block_a_pass():
    # Input hashing is a choice: an input step with no hash is NOT RECORDED.
    unhashed = record(run_id="pandas")
    unhashed["inputs"][0]["data_hash"] = None
    o = observe_record(unhashed, workload=W, environment="pandas")
    m = compare([obs("spark"), o], reference="spark")
    assert m["environments"]["pandas"]["dimensions"]["inputs"] == NOT_RECORDED
    assert m["environments"]["pandas"]["verdict"] == PASS


def test_an_input_the_reference_read_but_the_candidate_did_not_fails():
    m = compare([obs("spark"), obs("pandas", inputs=False)], reference="spark")
    assert m["environments"]["pandas"]["dimensions"]["inputs"] == FAIL


def test_a_failed_run_fails_execute_and_compares_nothing():
    m = compare([obs("spark"), obs("glue", status="error")], reference="spark")
    dims = m["environments"]["glue"]["dimensions"]
    assert dims["execute"] == FAIL and dims["data"] == NOT_RUN


def test_a_failed_launch():
    o = skipped(workload=W, environment="glue", status="failed_to_launch", reason="IAM denied")
    m = compare([obs("spark"), o], reference="spark")
    assert m["environments"]["glue"]["verdict"] == FAIL
    assert m["environments"]["glue"]["reason"] == "IAM denied"


@pytest.mark.parametrize("status, verdict", [("not_run", NOT_RUN), ("unsupported", UNSUPPORTED)])
def test_not_executed_is_never_a_pass(status, verdict):
    o = skipped(workload=W, environment="azure", status=status, reason="no subscription")
    m = compare([obs("spark"), o], reference="spark")
    assert set(m["environments"]["azure"]["dimensions"].values()) == {verdict}


def test_an_expected_environment_without_evidence_is_not_run():
    m = compare([obs("spark")], reference="spark", expect=["spark", "aws-glue"])
    assert verdicts(m)["aws-glue"] == NOT_RUN
    assert "no observation" in m["environments"]["aws-glue"]["reason"]


def test_the_reference_must_have_succeeded():
    with pytest.raises(ValueError, match="did not run successfully"):
        compare([obs("spark", status="error"), obs("pandas")], reference="spark")
    with pytest.raises(ValueError, match="no observation for the reference"):
        compare([obs("pandas")], reference="spark")


def test_one_workload_at_a_time():
    other = observe_record(record(), workload="c02", environment="x")
    with pytest.raises(ValueError, match="more than one workload"):
        compare([obs("spark"), other], reference="spark")


def test_observations_validate_status_and_cost_basis():
    with pytest.raises(ValueError):
        Observation(workload=W, environment="e", status="green")
    with pytest.raises(ValueError):
        Observation(workload=W, environment="e", status="not_run", cost={"basis": "guess"})
    with pytest.raises(ValueError):
        Observation(workload=W, environment="e", status="executed")  # no record


def test_an_estimate_is_labelled_never_a_bill():
    est = observe_record(
        record(run_id="g"),
        workload=W,
        environment="glue",
        cost={"basis": "ubunye_estimate", "amount": 0.02, "currency": "USD"},
    )
    billed = observe_record(
        record(run_id="d"),
        workload=W,
        environment="dataproc",
        cost={"basis": "actual", "amount": 0.05, "currency": "USD"},
    )
    table = render_markdown(compare([obs("spark"), est, billed], reference="spark"))
    assert "0.02 USD (est.)" in table and "0.05 USD (billed)" in table
    assert "unknown" in table or "local" in table


def test_save_and_load_round_trip(tmp_path):
    obs("spark").save(tmp_path)
    skipped(workload=W, environment="azure", status="not_run", reason="x").save(tmp_path)
    got = {o.environment: o.status for o in load_observations(tmp_path, W)}
    assert got == {"spark": "executed", "azure": "not_run"}


def test_the_cli_observes_skips_and_reports(tmp_path):
    runner = CliRunner()
    for env, data in (("spark", "d1"), ("pandas", "d1"), ("glue", "d2")):
        f = tmp_path / f"{env}.json"
        f.write_text(json.dumps(record(run_id=env, data=data)), encoding="utf-8")
        r = runner.invoke(
            app,
            [
                "prove",
                "observe",
                "--workload",
                W,
                "--env",
                env,
                "--record",
                str(f),
                "-o",
                str(tmp_path / "ev"),
            ],
        )
        assert r.exit_code == 0, r.output
    r = runner.invoke(
        app,
        [
            "prove",
            "skip",
            "--workload",
            W,
            "--env",
            "azure",
            "--reason",
            "no credentials",
            "-o",
            str(tmp_path / "ev"),
        ],
    )
    assert r.exit_code == 0, r.output
    r = runner.invoke(
        app,
        [
            "prove",
            "report",
            str(tmp_path / "ev"),
            "--workload",
            W,
            "--reference",
            "spark",
            "--expect",
            "databricks",
            "--json",
            str(tmp_path / "m.json"),
            "--md",
            str(tmp_path / "m.md"),
        ],
    )
    assert r.exit_code == 1  # glue disagrees
    m = json.loads((tmp_path / "m.json").read_text(encoding="utf-8"))
    assert verdicts(m) == {
        "spark": PASS,
        "azure": NOT_RUN,
        "databricks": NOT_RUN,
        "glue": FAIL,
        "pandas": PASS,
    }
    assert "no credentials" in (tmp_path / "m.md").read_text(encoding="utf-8")


def test_the_same_code_in_different_time_zones_is_not_the_same_run():
    utc = record(run_id="spark")
    utc["time_zone"] = "UTC"
    local = record(run_id="laptop", data="d-shifted")
    local["time_zone"] = "Africa/Johannesburg"
    m = compare(
        [
            observe_record(utc, workload=W, environment="spark"),
            observe_record(local, workload=W, environment="laptop"),
        ],
        reference="spark",
    )
    laptop = m["environments"]["laptop"]
    assert laptop["dimensions"]["identity"] == FAIL
    assert "time zone Africa/Johannesburg, reference UTC" in laptop["reason"]


def test_a_pandas_run_records_its_time_zone(tmp_path):
    pytest.importorskip("pandas")
    pytest.importorskip("pyarrow")
    import ubunye
    from ubunye.lineage.context import RunContext
    from ubunye.lineage.storage import FileSystemLineageStore

    task = tmp_path / "uc" / "pkg" / "t"
    task.mkdir(parents=True)
    (tmp_path / "in.csv").write_text("id\n1\n", encoding="utf-8")
    (task / "transformations.py").write_text(
        "from ubunye.core.interfaces import Task\n\n\n"
        "class T(Task):\n"
        "    def transform(self, sources):\n"
        "        return {'out': sources['src']}\n",
        encoding="utf-8",
    )
    (task / "config.yaml").write_text(
        'MODEL: etl\nVERSION: "1.0.0"\nCONFIG:\n  inputs:\n    src:\n      format: s3\n'
        f"      path: \"{(tmp_path / 'in.csv').as_posix()}\"\n      file_format: csv\n"
        "  transform: {}\n  outputs:\n    out:\n      format: s3\n"
        f"      path: \"{(tmp_path / 'out').as_posix()}\"\n      file_format: parquet\n"
        "      mode: overwrite\n",
        encoding="utf-8",
    )
    ubunye.run_task(str(task), backend="pandas", lineage=True)
    (rec,) = FileSystemLineageStore(str(tmp_path / ".ubunye" / "lineage")).list_runs("uc/pkg/t")
    assert rec.time_zone == "UTC"
    old = rec.to_dict()
    del old["time_zone"]  # a record written before the field existed still loads
    assert RunContext.from_dict(old).time_zone is None


# --- F-025: one zone, two names --------------------------------------------------


@pytest.mark.parametrize(
    "a, b",
    [("UTC", "Etc/UTC"), ("UTC", "GMT"), ("utc", "Zulu"), ("Etc/UTC", "Etc/Universal")],
)
def test_utc_spellings_are_the_same_zone(a, b):
    from ubunye.proving.matrix import same_zone

    assert same_zone(a, b) and same_zone(b, a)


def test_different_zones_stay_different():
    from ubunye.proving.matrix import same_zone

    assert not same_zone("UTC", "Africa/Johannesburg")
    assert not same_zone("UTC", "Europe/London")  # summer time differs
    assert not same_zone("UTC", "Not/AZone")  # unknown: not provably the same


def test_databricks_etc_utc_is_the_same_run_as_utc():
    # Found live (F-025): Databricks records Etc/UTC, Spark and pandas UTC; the data
    # was identical and the proving report still said identity FAIL.
    spark = record(run_id="spark")
    spark["time_zone"] = "UTC"
    dbx = record(run_id="databricks")
    dbx["time_zone"] = "Etc/UTC"
    m = compare(
        [
            observe_record(spark, workload=W, environment="spark"),
            observe_record(dbx, workload=W, environment="databricks"),
        ],
        reference="spark",
    )
    assert m["environments"]["databricks"]["verdict"] == PASS
    assert m["environments"]["databricks"]["reason"] == ""
