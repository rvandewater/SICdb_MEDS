"""The code-metadata tables, the config blocks that read them, and the final codes.parquet stay in step."""

from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from meds import code_metadata_schema
from MEDS_transforms.extract.extract_code_metadata import (
    extract_all_metadata,
    get_events_and_metadata_by_metadata_fp,
)
from omegaconf import OmegaConf

from SICdb_MEDS.code_metadata import (
    STATIC_DESCRIPTIONS_CSV,
    finalize_code_metadata,
    read_static_code_descriptions,
    write_metadata_tables,
)

CONFIG_FP = (
    Path(__file__).parents[1] / "src" / "SICdb_MEDS" / "configs" / "event_configs.yaml"
)

# Synthetic stand-ins for the pre-MEDS tables; no SICdb content.
CASES = pl.DataFrame(
    {
        "ICD10Main": ["I10", "S62.60", "X1.x", None],
        "ICD10MainText": [
            "hypertension",
            "finger fracture",
            "malformed code",
            "no code",
        ],
    }
)
LABORATORY = pl.DataFrame(
    {
        "LOINC_code": ["2160-0", None, "LOCAL-1"],
        "ReferenceUnit": ["mg/dl", "U/l", None],
        "ReferenceValue": ["Creatinine", "Enzyme", "Local test"],
    }
)
REFERENCES = pl.DataFrame(
    {
        "ReferenceGlobalID": [1, 2, 3, 735],
        "ReferenceName": ["Signal group", "Drug group", "Lab group", "Sex group"],
        "ReferenceValue": ["Heart rate", "Drug X", "Creatinine", "Synthetic sex"],
        "ReferenceUnit": ["bpm", None, "mg/dl", None],
        "LOINC_code": [None, None, "2160-0", None],
    }
)


def _events():
    cfg = OmegaConf.to_container(OmegaConf.load(CONFIG_FP))
    for table, events in cfg.items():
        if table == "subject_id_col":
            continue
        for name, event in events.items():
            yield table, name, event


def _code_parts(event) -> list[str]:
    code = event["code"]
    return [code] if isinstance(code, str) else list(code)


def _is_literal(event) -> bool:
    return not any(part.startswith("col(") for part in _code_parts(event))


@pytest.fixture
def pre_MEDS_dir(tmp_path):
    CASES.write_parquet(tmp_path / "cases.parquet")
    LABORATORY.write_parquet(tmp_path / "laboratory.parquet")
    write_metadata_tables(tmp_path, REFERENCES.lazy())
    return tmp_path


def test_static_table_is_complete_and_sourced():
    df = pl.read_csv(STATIC_DESCRIPTIONS_CSV, infer_schema=False)
    assert df.columns == ["code", "description", "source"]
    assert not df["code"].is_duplicated().any()
    assert df.null_count().sum_horizontal().item() == 0
    assert df["description"].str.len_chars().min() > 10
    assert (
        df["source"]
        .str.contains(r"https://www\.sicdb\.com/|pre_MEDS\.|MEDS-Transforms")
        .all()
    )


def test_every_literal_code_is_described():
    static = set(read_static_code_descriptions()["code"])
    literal = {"//".join(_code_parts(e)) for _, _, e in _events() if _is_literal(e)}
    assert literal, "no literal codes found; the config parsing is stale"
    assert literal <= static, sorted(literal - static)
    # The code of a case without ICD10Main, which MEDS-Transforms fills with UNK.
    assert static - literal == {"DIAGNOSIS//ICD10//UNK"}


def test_every_component_coded_event_reads_a_metadata_table(pre_MEDS_dir):
    for table, name, event in _events():
        if _is_literal(event):
            continue
        blocks = event.get("_metadata", {})
        assert any("description" in b for b in blocks.values()), (
            f"{table}/{name} has no description"
        )
        for prefix, block in blocks.items():
            columns = pl.read_parquet_schema(pre_MEDS_dir / f"{prefix}.parquet")
            needed = set(block.values()) | {
                p[4:-1] for p in _code_parts(event) if p.startswith("col(")
            }
            assert needed <= set(columns), (table, name, needed - set(columns))


def test_config_blocks_produce_the_expected_metadata(pre_MEDS_dir):
    """Run MEDS-Transforms' own metadata extraction on the synthetic tables."""
    by_prefix = get_events_and_metadata_by_metadata_fp(OmegaConf.load(CONFIG_FP))
    assert set(by_prefix) == {
        "diagnosis_metadata",
        "laboratory_metadata",
        "reference_metadata",
    }
    out = pl.concat(
        [
            extract_all_metadata(
                pl.scan_parquet(pre_MEDS_dir / f"{p}.parquet"), cfgs
            ).collect()
            for p, cfgs in by_prefix.items()
        ],
        how="diagonal_relaxed",
    )
    # Vocabulary parents come from suMEDS (`sumeds_rules.yaml`, tests/test_sumeds_rules.py).
    assert "parent_codes" not in out.columns
    got = dict(zip(out["code"], out["description"], strict=True))
    assert got["DIAGNOSIS//ICD10//I10"] == "hypertension"
    assert got["DIAGNOSIS//ICD10//S62.60"] == "finger fracture"
    assert got["DIAGNOSIS//ICD10//X1.x"] == "malformed code"
    assert "DIAGNOSIS//ICD10//UNK" not in got  # described by the static table instead
    assert got["LAB//LOINC//2160-0//mg/dl"] == "Creatinine, mg/dl (laboratory result)"
    assert got["LAB//LOINC//UNK//U/l"] == "Enzyme, U/l (laboratory result)"
    assert got["LAB//LOINC//LOCAL-1//UNK"] == "Local test (laboratory result)"
    assert got["VITAL//Heart rate//bpm"] == "Heart rate, bpm (hourly aggregated signal)"
    assert got["MEDICATION_END//Drug group//Drug X//UNK"].startswith(
        "Drug X (end of drug administration"
    )
    assert got["MEDICATION//Drug group"].startswith("Start of a drug administration")
    # ReferenceValue of reference 735
    assert got["SEX//735"] == "Sex: Synthetic sex (cases.Sex)"
    assert out["code"].is_unique().all()


def test_stale_laboratory_table_is_rejected(tmp_path):
    LABORATORY.drop("ReferenceValue").write_parquet(tmp_path / "laboratory.parquet")
    with pytest.raises(ValueError, match="ReferenceValue"):
        write_metadata_tables(tmp_path, REFERENCES.lazy())


def test_finalize_code_metadata(tmp_path):
    (tmp_path / "data" / "train").mkdir(parents=True)
    (tmp_path / "metadata").mkdir()
    data_codes = [
        "MEDS_BIRTH",
        "WEIGHT",
        "HEIGHT",
        "LAB//LOINC//2160-0//mg/dl",
        "VITAL//X//UNK",
    ]
    pl.DataFrame(
        {"subject_id": range(len(data_codes)), "code": data_codes}
    ).write_parquet(tmp_path / "data" / "train" / "0.parquet")
    # As the pipeline leaves it: only extracted codes, written by polars (large_string).
    pl.DataFrame(
        {
            "code": ["LAB//LOINC//2160-0//mg/dl", "WEIGHT"],
            "description": ["Creatinine, mg/dl (laboratory result)", "extracted wins"],
            "parent_codes": [["LOINC/2160-0", "LOINC/2160-0", None], []],
        }
    ).write_parquet(tmp_path / "metadata" / "codes.parquet")

    counts = finalize_code_metadata(tmp_path)

    fp = tmp_path / "metadata" / "codes.parquet"
    table = pq.read_table(fp)
    assert table.schema.equals(code_metadata_schema())
    out = {r["code"]: (r["description"], r["parent_codes"]) for r in table.to_pylist()}
    assert set(out) == set(data_codes)
    assert out["LAB//LOINC//2160-0//mg/dl"] == (
        "Creatinine, mg/dl (laboratory result)",
        ["LOINC/2160-0"],
    )
    assert out["WEIGHT"] == ("extracted wins", None)
    assert out["MEDS_BIRTH"][0].startswith("Birth")
    assert out["HEIGHT"][0].startswith("Body height on admission, cm")
    assert out["VITAL//X//UNK"] == (None, None)
    assert counts == {"codes": 5, "described": 4, "with_parents": 1}
    assert finalize_code_metadata(tmp_path) == counts  # idempotent


def test_finalize_code_metadata_without_parent_codes(tmp_path):
    """No block emits parent codes, so the extracted file may lack the column."""
    (tmp_path / "data" / "train").mkdir(parents=True)
    (tmp_path / "metadata").mkdir()
    data_codes = ["MEDS_BIRTH", "DIAGNOSIS//ICD10//I10"]
    pl.DataFrame({"subject_id": [1, 2], "code": data_codes}).write_parquet(
        tmp_path / "data" / "train" / "0.parquet"
    )
    pl.DataFrame(
        {"code": ["DIAGNOSIS//ICD10//I10"], "description": ["hypertension"]}
    ).write_parquet(tmp_path / "metadata" / "codes.parquet")

    counts = finalize_code_metadata(tmp_path)

    table = pq.read_table(tmp_path / "metadata" / "codes.parquet")
    assert table.schema.equals(code_metadata_schema())
    assert table.column("parent_codes").null_count == 2
    assert counts == {"codes": 2, "described": 2, "with_parents": 0}
