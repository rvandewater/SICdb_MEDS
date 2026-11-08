"""`meds-extract-run spec=SICdb` runs the ETL and then suMEDS enrichment, offline.

The spec declares suMEDS's `sumeds_enrich` post-stage with the rules this package registers in
suMEDS's `sumeds.rules` entry-point group under the same name, so the one command also enriches the
output; ``conftest.py``'s plain run leaves the stage out with `skip_stages=[sumeds_enrich]`. The
ontology source is a tiny pack repository in suMEDS's `sumeds-ontologies/0.1` layout, built here
with a LOINC pack that holds a made-up title for the one synthetic LOINC code of ``conftest.py``,
so the test reads nothing over the network and does not depend on the published packs. It reaches
the stage as a pipeline override, `"overrides=['++stage_cfg.ontology_source=DIR']"`. suMEDS has no
ICD-10-GM pack, so the diagnoses gain only the same-dataset parents of the packaged rule. Another
run points at an empty pack source, through `SUMEDS_ONTOLOGY_SOURCE`, which is how a site without
internet fails: the ETL still succeeds, unenriched, and `suMEDS-enrich` completes it later.
"""

import csv
import hashlib
import io
import json
import shutil
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from conftest import etl_args, run_command
from meds import CodeMetadataSchema, DataSchema, DatasetMetadataSchema

LOINC_RELEASE = "2.83"
LOINC_TITLES = {"2160-0": "Synthetic LOINC title 2160-0"}


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def names(value) -> dict[str, str]:
    """A names map (suMEDS >= 0.6) as a dict.

    Parquet stores it as `map<string, string>`: Polars 2 reads that as a dict, Polars 1 as a list
    of key/value structs.
    """
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    return {entry["key"]: entry["value"] for entry in value}


def _dataset(output_dir: Path) -> dict:
    """`dataset.json`, checked to still be valid MEDS dataset metadata."""
    dataset = json.loads((output_dir / "metadata" / "dataset.json").read_text())
    DatasetMetadataSchema.validate(dataset)
    return dataset


@pytest.fixture(scope="session")
def ontology_source(tmp_path_factory) -> Path:
    """A local `sumeds-ontologies/0.1` repository holding a synthetic LOINC pack.

    Like the published pack, it keeps the LOINC Table's field names and has no parent column:
    LOINC terms are leaves.
    """
    root = tmp_path_factory.mktemp("ontologies")
    directory = root / "LOINC" / LOINC_RELEASE
    directory.mkdir(parents=True)
    columns = ["LOINC_NUM", "LONG_COMMON_NAME"]
    by_shard: dict[str, list[list[str]]] = {}
    for code, title in LOINC_TITLES.items():
        by_shard.setdefault(code[:2], []).append([code, title])
    shards = {}
    for key, rows in by_shard.items():
        buffer = io.StringIO()
        csv.writer(buffer, lineterminator="\n").writerows([columns, *rows])
        data = buffer.getvalue().encode()
        (directory / f"{key}.csv").write_bytes(data)
        shards[key] = {
            "file": f"{key}.csv",
            "rows": len(rows),
            "bytes": len(data),
            "sha256": _sha256(data),
        }
    manifest = {
        "schema": "sumeds-ontology-pack/0.1",
        "vocabulary": "LOINC",
        "release": LOINC_RELEASE,
        "columns": columns,
        "fields": {
            "code": "LOINC_NUM",
            "description": "LONG_COMMON_NAME",
            "parent": None,
        },
        "shard_key_length": 2,
        "rows": len(LOINC_TITLES),
        "bytes": sum(s["bytes"] for s in shards.values()),
        "shards": shards,
    }
    manifest_bytes = json.dumps(manifest).encode()
    (directory / "manifest.json").write_bytes(manifest_bytes)
    index = {
        "schema": "sumeds-ontologies/0.1",
        "vocabularies": {
            "LOINC": {
                "manifest": f"LOINC/{LOINC_RELEASE}/manifest.json",
                "sha256": _sha256(manifest_bytes),
                "release": LOINC_RELEASE,
                "rows": manifest["rows"],
                "bytes": manifest["bytes"],
            }
        },
    }
    (root / "index.json").write_text(json.dumps(index))
    return root


def stage_options(**options: object) -> str:
    """Set `sumeds_enrich` options for one run.

    The override reaches every stage of the pipeline, so it adds the key (`++`) rather than setting one
    that only this stage declares.
    """
    values = ",".join(
        f"'++stage_cfg.{name}={value}'" for name, value in options.items()
    )
    return f"overrides=[{values}]"


@pytest.fixture(scope="session")
def enriched_output(tmp_path_factory, raw_dir: Path, ontology_source: Path) -> Path:
    output_dir = tmp_path_factory.mktemp("sicdb_enriched") / "MEDS_cohort"
    options = stage_options(ontology_source=ontology_source)
    run_command([*etl_args(output_dir, raw_dir), options])
    return output_dir


@pytest.fixture(scope="session")
def enriched_codes(enriched_output: Path) -> pl.DataFrame:
    return pl.read_parquet(enriched_output / "metadata" / "codes.parquet")


def test_codes_parquet_is_replaced_by_valid_meds_metadata(enriched_output: Path):
    metadata = enriched_output / "metadata"
    table = pq.read_table(metadata / "codes.parquet")
    CodeMetadataSchema.validate(table)
    assert table.schema.field("parent_names").type == pa.map_(pa.string(), pa.string())
    assert not list(metadata.glob(".*enriched*")), (
        "the temporary enrichment output was left behind"
    )


def test_data_matches_the_plain_etl(enriched_output: Path, events: pl.DataFrame):
    enriched_events = pl.concat(
        [pl.read_parquet(fp) for fp in (enriched_output / "data").rglob("*.parquet")]
    )
    key = ["subject_id", "time", "code"]
    assert enriched_events.sort(key).select(key).equals(events.sort(key).select(key))


def test_enrichment_keeps_the_etl_metadata(
    codes: pl.DataFrame, enriched_codes: pl.DataFrame
):
    """Every value the ETL wrote is kept, and its `parent_codes` stay a prefix of the new list."""
    assert codes["parent_codes"].drop_nulls().len() > 0, (
        "the ETL wrote no parents to keep"
    )
    enriched = {row["code"]: row for row in enriched_codes.to_dicts()}
    assert set(enriched) == set(codes["code"])
    for row in codes.to_dicts():
        new = enriched[row["code"]]
        parents = row.pop("parent_codes") or []
        assert (new["parent_codes"] or [])[: len(parents)] == parents, row["code"]
        assert {column: new[column] for column in row} == row


def test_diagnoses_get_same_dataset_parents(enriched_codes: pl.DataFrame):
    rows = {r["code"]: r for r in enriched_codes.to_dicts()}
    parents = {
        code: rows[f"DIAGNOSIS//ICD10GM//{code}"]["parent_codes"]
        for code in ["S62", "S62.6", "S62.60", "E11.9", "C19.x1"]
    }
    assert parents == {
        "S62": ["ICD10GM/S62"],
        # The ETL's ICD10GM/ parent first, then the nearest shorter code in the data.
        "S62.6": ["ICD10GM/S62.6", "DIAGNOSIS//ICD10GM//S62"],
        "S62.60": ["ICD10GM/S62.60", "DIAGNOSIS//ICD10GM//S62.6"],
        # E11 does not occur, and a non-canonical code takes no part.
        "E11.9": ["ICD10GM/E11.9"],
        "C19.x1": None,
    }
    # Without an ICD-10-GM source, nothing is named: same-dataset parents are described in their
    # own rows.
    assert not names(rows["DIAGNOSIS//ICD10GM//S62.60"]["parent_names"])


def test_loinc_parents_get_their_names(enriched_codes: pl.DataFrame):
    rows = {r["code"]: r for r in enriched_codes.to_dicts()}
    for code in ["LAB//2001//UNSPEC", "LAB//2001//ARTERIAL"]:
        lab = rows[code]
        assert (
            lab["description"] == "Synthetic analyte A"
        )  # d_references, not the LOINC title
        assert lab["parent_codes"] == ["LOINC/2160-0"]
        assert names(lab["parent_names"]) == {"LOINC/2160-0": LOINC_TITLES["2160-0"]}
        assert (lab["vocabulary_id"], lab["concept_code"]) == ("LOINC", "2160-0")

    # A local id in d_references.LOINC_code is no LOINC parent, so there is nothing to look up.
    local = rows["LAB//2002//UNSPEC"]
    assert not local["parent_codes"]
    assert local["vocabulary_id"] is None


def test_enriching_an_existing_build_gives_the_same_codes(
    tmp_path: Path,
    meds_output: Path,
    ontology_source: Path,
    enriched_codes: pl.DataFrame,
):
    """`suMEDS-enrich ... --rules SICdb` on the plain build matches the spec's enrichment stage."""
    codes_fp = tmp_path / "codes.parquet"
    shutil.copy(meds_output / "metadata" / "codes.parquet", codes_fp)
    run_command(
        [
            "suMEDS-enrich",
            str(codes_fp),
            "-o",
            str(codes_fp),
            "--rules",
            "SICdb",
            "--ontology-source",
            str(ontology_source),
        ]
    )
    assert pl.read_parquet(codes_fp).sort("code").equals(enriched_codes.sort("code"))


def test_dataset_json_records_the_enrichment(
    enriched_output: Path,
    meds_output: Path,
    codes: pl.DataFrame,
    enriched_codes: pl.DataFrame,
):
    """`metadata/dataset.json` says the build is enriched, with which rules, and what it added."""
    dataset, plain = _dataset(enriched_output), _dataset(meds_output)
    assert "sumeds_enrichment" not in plain
    assert set(dataset) == {*plain, "sumeds_enrichment"}
    record = dataset["sumeds_enrichment"]
    assert record["enriched"] is True
    assert record["rules"] == "SICdb"
    assert record["ontology_source"] == "local"
    assert record["athena"] is False

    def filled(frame: pl.DataFrame, column: str) -> int:
        return frame[column].drop_nulls().len()

    assert record["coverage"] == {
        "codes": enriched_codes.height,
        "described": [
            filled(codes, "description"),
            filled(enriched_codes, "description"),
        ],
        "with_parent_codes": [
            filled(codes, "parent_codes"),
            filled(enriched_codes, "parent_codes"),
        ],
    }


def test_skip_stages_runs_the_plain_etl(meds_output: Path):
    """`skip_stages=[sumeds_enrich]` leaves the stage out: it never runs, and nothing is recorded."""
    assert "sumeds_enrichment" not in _dataset(meds_output)
    assert not (meds_output / ".logs" / "sumeds_enrich.done").exists()
    pipeline = (meds_output / ".meds_extract_run" / "pipeline.yaml").read_text()
    assert "sumeds_enrich" not in pipeline


def test_dataset_json_describes_the_release_and_its_columns(
    meds_output: Path, events: pl.DataFrame
):
    """The `etl:` descriptors and extension-column categories are written to `dataset.json`, and
    every other extension column is listed under `other_extension_columns`."""
    dataset = _dataset(meds_output)
    assert (
        dataset["license"] == "PhysioNet Contributor Review Health Data License 1.5.0"
    )
    assert dataset["description_uri"] == "https://physionet.org/content/sicdb/1.0.8/"
    assert dataset["raw_source_id_columns"] == ["CaseID"]
    extension = set(events.columns) - set(DataSchema.schema().names)
    assert set(dataset["other_extension_columns"]) == extension - {"CaseID"}
    assert "code_modifier_columns" not in dataset


def test_enrichment_soft_fails_and_suMEDS_enrich_completes_it_later(
    tmp_path: Path,
    raw_dir: Path,
    ontology_source: Path,
    meds_output: Path,
    enriched_output: Path,
    enriched_codes: pl.DataFrame,
):
    """With no usable pack source (as on a site without internet), the ETL still succeeds:
    `codes.parquet` is left exactly as the plain ETL writes it, `dataset.json` records that it is
    not enriched and how to retry, and `suMEDS-enrich <OUT>` then gives the same table as a run
    that enriched directly.

    Running the ETL again does not retry: the stage succeeded, so MEDS-Extract skips it.
    """
    empty_source = tmp_path / "no_packs"
    empty_source.mkdir()
    output_dir = tmp_path / "MEDS_cohort"
    args = etl_args(output_dir, raw_dir)
    run_command(args, env={"SUMEDS_ONTOLOGY_SOURCE": str(empty_source)})

    # The stage's warning goes to the pipeline log, not to the console.
    log = (output_dir / ".logs" / "pipeline.log").read_text()
    assert "enrichment failed" in log
    assert f"suMEDS-enrich {output_dir}" in log

    codes_fp = output_dir / "metadata" / "codes.parquet"
    plain_fp = meds_output / "metadata" / "codes.parquet"
    assert codes_fp.read_bytes() == plain_fp.read_bytes()
    record = _dataset(output_dir)["sumeds_enrichment"]
    assert record["enriched"] is False
    assert record["rules"] == "SICdb"
    assert record["error"].startswith("OntologyPackError")
    assert record["retry"] == "suMEDS-enrich <MEDS_ROOT>"

    run_command([*args, stage_options(ontology_source=ontology_source)])
    assert _dataset(output_dir)["sumeds_enrichment"] == record

    run_command(
        ["suMEDS-enrich", str(output_dir), "--ontology-source", str(ontology_source)]
    )
    record = _dataset(output_dir)["sumeds_enrichment"]
    direct = _dataset(enriched_output)["sumeds_enrichment"]
    assert record["enriched"] is True
    assert {k: v for k, v in record.items() if k != "enriched_at"} == {
        k: v for k, v in direct.items() if k != "enriched_at"
    }
    assert pl.read_parquet(codes_fp).sort("code").equals(enriched_codes.sort("code"))


def test_strict_makes_a_failed_enrichment_fail_the_run(tmp_path: Path, raw_dir: Path):
    empty_source = tmp_path / "no_packs"
    empty_source.mkdir()
    output_dir = tmp_path / "MEDS_cohort"
    options = stage_options(ontology_source=empty_source, strict="true")
    run_command([*etl_args(output_dir, raw_dir), options], ok=False)
    assert not (output_dir / ".logs" / "sumeds_enrich.done").exists()
    assert "sumeds_enrichment" not in _dataset(output_dir)
