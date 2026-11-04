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


def test_literal_codes_use_constant_columns(cfg: MessyConfig):
    """The literal codes described through a constant column keep their exact spelling."""
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


OUTPUT_DIR = os.environ.get("SICDB_MEDS_OUTPUT_DIR")


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
