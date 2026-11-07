"""`suMEDS-extract spec=SICdb` runs the ETL and then suMEDS enrichment, offline.

This package registers its rules in suMEDS's `sumeds.rules` entry-point group under the same name
as its MEDS-Extract spec, so `suMEDS-extract` finds them from `spec=SICdb` alone. The ontology
source is a tiny pack repository in suMEDS's `sumeds-ontologies/0.1` layout, built here with a
LOINC pack that holds a made-up title for the one synthetic LOINC code of ``conftest.py``, so the
test reads nothing over the network and does not depend on the published packs. suMEDS has no
ICD-10-GM pack, so the diagnoses gain only the same-dataset parents of the packaged rule.
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
from conftest import run_command
from meds import CodeMetadataSchema

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


@pytest.fixture(scope="session")
def enriched_output(tmp_path_factory, raw_dir: Path, ontology_source: Path) -> Path:
    output_dir = tmp_path_factory.mktemp("sicdb_enriched") / "MEDS_cohort"
    run_command(
        [
            "suMEDS-extract",
            "spec=SICdb",
            f"output_dir={output_dir}",
            "do_download=false",
            f"input_dir={raw_dir}",
            "--",
            "--ontology-source",
            str(ontology_source),
        ]
    )
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
    """`suMEDS-enrich ... --rules SICdb` on the plain build matches `suMEDS-extract`."""
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
