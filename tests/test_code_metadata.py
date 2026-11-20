"""Code metadata: every event block describes the codes it emits.

``test_every_event_is_described`` needs no data. ``test_built_codes_metadata`` checks a finished
extraction and runs only when ``SICDB_MEDS_OUTPUT_DIR`` points at one (the ``output_dir`` of
``meds-extract-run``). It reports counts and code families only, never codes or descriptions.
"""

import os
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
import pytest
from MEDS_extract.config import MessyConfig
from meds import CodeMetadataSchema

from SICdb_MEDS import MESSY_CFG


@pytest.fixture(scope="module")
def cfg() -> MessyConfig:
    return MessyConfig.load(MESSY_CFG)


def test_every_event_is_described(cfg: MessyConfig):
    """Each event block has a metadata block (`_self` or a dictionary table) with a description.

    MEDS-Extract 0.8 rejects a `_self` block on a bare literal code, so a literal is read from a
    constant `_table.cols` column instead (see the `cases` table of messy.yaml).
    """
    undescribed = [
        f"{table.input_prefix}/{event.name}"
        for table in cfg.event_tables
        for event in table.events
        if not any("description" in block for block in event.metadata.values())
    ]
    assert not undescribed, f"event blocks without a description: {undescribed}"


def test_literal_codes_keep_their_spelling(cfg: MessyConfig):
    """The literal codes (plain literals with a `_self` block, MEDS-Extract >= 0.8.1) keep their exact
    spelling."""
    cases = next(t for t in cfg.event_tables if t.input_prefix == "cases")
    events = {event.name: event for event in cases.events}
    expected = {
        "death": "MEDS_DEATH",
        "icu_admission": "ICU_ADMISSION",
        "heart_surgery_begin": "HEART_SURGERY_BEGIN",
        "heart_surgery_end": "HEART_SURGERY_END",
        "heart_surgery_cpb_time": "HEART_SURGERY_CPB_MINUTES",
        "heart_surgery_crossclamp_time": "HEART_SURGERY_CROSSCLAMP_MINUTES",
    }
    codes = (
        cases.prepare(pl.LazyFrame({c: [0] for c in sorted(cases.source_columns())}))
        .select(
            **{name: events[name].polars_exprs["code"] for name in expected},
        )
        .collect()
        .to_dicts()[0]
    )
    assert codes == expected


def test_built_codes_have_one_string_unit(codes: pl.DataFrame):
    """On the synthetic build: dictionary units, the literals the descriptions state, and none else.

    MEDS-Extract >= 0.9 writes `unit` as one String per code, not a list (MEDS_extract#336).
    suMEDS's `sumeds_normalize_units` then writes `unit` in UCUM and keeps the spelling as recorded
    in `unit_source_value`.
    """
    assert codes["unit"].dtype == pl.String
    unit = dict(zip(codes["code"].to_list(), codes["unit"].to_list(), strict=True))
    recorded = dict(
        zip(codes["code"].to_list(), codes["unit_source_value"].to_list(), strict=True)
    )
    assert unit["LAB//2001//UNSPEC"] == "mg/dL"
    assert recorded["LAB//2001//UNSPEC"] == "mg/dl"
    assert (unit["VITAL//5001"], recorded["VITAL//5001"]) == ("{beats}/min", "bpm")
    assert unit["VITAL//708"] == "/min"
    assert unit["MEDICATION//3001"] == "mg"
    assert (unit["WEIGHT"], unit["HEIGHT"]) == ("kg", "cm")
    assert unit["HEART_SURGERY_CPB_MINUTES"] == "min"
    assert unit["LAB//2004//UNSPEC"] is None  # no ReferenceUnit
    assert unit["SAPS3"] is None  # a score
    # No SICdb code ends with its unit, so there are no unit siblings to link.
    assert not [c for c, u in unit.items() if u and c.endswith(f"//{u}")]
    assert not [c for c, u in recorded.items() if u and c.endswith(f"//{u}")]


OUTPUT_DIR = os.environ.get("SICDB_MEDS_OUTPUT_DIR")


def test_config_loinc_parents(codes: pl.DataFrame):
    """LOINC parents the config adds (vitals, WEIGHT, HEIGHT) and the one it withholds (8310-5 on a lab)."""
    rows = {r["code"]: r for r in codes.to_dicts()}
    assert rows["VITAL//708"]["parent_codes"] == ["LOINC/8867-4"]
    # Any other DataID keeps no parent.
    assert not rows["VITAL//5001"]["parent_codes"]
    assert rows["WEIGHT"]["parent_codes"] == ["LOINC/29463-7"]
    assert rows["HEIGHT"]["parent_codes"] == ["LOINC/8302-2"]
    # A lab item with the body-temperature LOINC keeps its description but gets no parent.
    assert not rows["LAB//2004//UNSPEC"]["parent_codes"]
    assert rows["LAB//2004//UNSPEC"]["description"] == "Synthetic analyte D"


@pytest.mark.skipif(
    not OUTPUT_DIR, reason="set SICDB_MEDS_OUTPUT_DIR to a finished extraction"
)
def test_built_codes_metadata():
    """codes.parquet follows the MEDS schema and describes every code in the data."""
    root = Path(OUTPUT_DIR)
    codes_fp = root / "metadata" / "codes.parquet"
    CodeMetadataSchema.validate(pq.read_table(codes_fp))

    data_codes = (
        pl.scan_parquet(root / "data" / "**" / "*.parquet").select("code").unique()
    )
    described = pl.scan_parquet(codes_fp).filter(
        pl.col("description").is_not_null() & (pl.col("description") != "")
    )
    missing = (
        data_codes.join(described.select("code"), on="code", how="anti")
        .select(pl.col("code").str.split("//").list.first().alias("family"))
        .group_by("family")
        .len()
        .sort("family")
        .collect()
    )
    assert missing.is_empty(), (
        f"{missing['len'].sum()} codes have no description, by family: "
        f"{dict(zip(missing['family'], missing['len']))}"
    )
