"""Validate the bundled MESSY config against the installed MEDS-Extract.

These tests need no raw data and no credentials, so they run in CI on every push. They are the
regression net for the 0.7 migration: a MESSY file that stops parsing (a bad dftly expression, a
stray 0.6.x key, an `etl:` option MEDS-Extract does not accept) fails here rather than several
stages into a multi-hour extraction run.
"""

import pytest

from MEDS_extract.config import MessyConfig

from SICdb_MEDS import MESSY_CFG, PIPELINE_NAME

EXPECTED_TABLES = {
    # There is no `patient` table in the release: the old pipeline synthesised one in pre-MEDS,
    # and its events are re-homed onto `cases` here.
    "cases": {
        "admission",
        "admission_year",
        "birth",
        "crrt_hours",
        "death",
        "diagnosis",
        "diagnosis_secondary_text",
        "discharge",
        "discharge_unit",
        "heart_surgery_begin",
        "heart_surgery_cpb_time",
        "heart_surgery_crossclamp_time",
        "heart_surgery_end",
        "height",
        "hospital_discharge",
        "hospital_stay_days",
        "hospital_unit",
        "icu_admission",
        "referring_unit",
        "saps3",
        "sepsis_on_admission",
        "sex",
        "surgical_admission_type",
        "surgical_site",
        "survival_observation_window",
        "weight",
    },
    "laboratory": {"lab"},
    "medication": {"administration", "infusion_rate", "medication_end"},
    "data_ref": {"baseline", "derived"},
    "data_range": {"device_start", "device_end"},
    "data_float_h": {"vital"},
}


@pytest.fixture(scope="module")
def cfg() -> MessyConfig:
    return MessyConfig.load(MESSY_CFG)


@pytest.fixture(scope="module")
def by_prefix(cfg: MessyConfig) -> dict:
    return {t.input_prefix: t for t in cfg.event_tables}


@pytest.fixture
def dummy_credentials(monkeypatch):
    """Satisfy the `${oc.env:...}` interpolations in the `sources:` block.

    `selected_sources()` resolves interpolations for the selected bucket, so inspecting the
    declared sources needs the credential vars to exist. The values are never used -- nothing here
    touches the network -- but without this the test only passes on a machine that happens to have
    real credentials exported.
    """
    monkeypatch.setenv("DATASET_DOWNLOAD_USERNAME", "not-a-real-user")
    monkeypatch.setenv("DATASET_DOWNLOAD_PASSWORD", "not-a-real-password")


def test_messy_config_parses(cfg: MessyConfig):
    """Every event table, code expression, and time cast in the config is valid dftly."""
    tables = cfg.event_tables
    assert tables, "MESSY config declares no event tables."
    for table in tables:
        assert table.events, f"Table {table.input_prefix!r} declares no events."


def test_expected_tables_and_events(by_prefix: dict):
    """The migrated config covers exactly the tables the 0.6.x event config covered."""
    assert {
        p: {e.name for e in t.events} for p, t in by_prefix.items()
    } == EXPECTED_TABLES


def test_subject_id_is_patient_id(cfg: MessyConfig):
    """Every table inherits the global `_defaults.subject_id`."""
    for table in cfg.event_tables:
        assert table.subject_id_node is not None, table.input_prefix
        assert table.subject_id_node.referenced_columns == {"PatientID"}


def test_value_columns_are_column_reads(by_prefix: dict):
    """`numeric_value`/`text_value` read columns, not bare-string literals.

    A bare string is a LITERAL in dftly, so every raw-column read must carry a `$` prefix. This
    fails silently (wrong data, no error) rather than loudly, so it is worth pinning.
    """
    lab = by_prefix["laboratory"].events[0].referenced_columns
    assert {"LaboratoryValue", "LaboratoryID"} <= lab

    vital = by_prefix["data_float_h"].events[0].referenced_columns
    assert {"Val", "DataID"} <= vital


def test_physionet_source_declares_unarchive(cfg: MessyConfig, dummy_credentials):
    """Archive members are expanded by the download layer, not by ETL code.

    `auto` infers the format per member and is a no-op on non-archives (`README.md`, `*.csv.gz`
    -- gz compression is not a tar archive), so declaring it source-level is safe even though the
    release mixes archive and non-archive members under one SHA256SUMS.
    """
    sources = cfg.selected_sources("dataset")
    assert sources, "no sources declared in the `dataset` bucket"
    for src in sources:
        assert getattr(src, "_unarchive", None) == "auto", type(src).__name__


def test_etl_block(cfg: MessyConfig):
    """The reserved `etl:` block carries the dataset identity and stage options."""
    assert cfg.etl.dataset_name == "SICdb"
    assert cfg.etl.stage_options["n_subjects_per_shard"] == 1000


def test_sources_declare_dataset_version(cfg: MessyConfig):
    """`sources.dataset_version` is what stamps `etl_metadata.dataset_version` on the output."""
    assert cfg.sources_version == "1.0.8"


def test_registered_pipeline_name_resolves():
    """The `MEDS_extract.pipelines` entry point resolves to the bundled MESSY file.

    This is what makes `meds-extract-run spec=SICdb output_dir=...` work.
    """
    cfg = MessyConfig.load(PIPELINE_NAME)
    assert cfg.registered_name == PIPELINE_NAME
    assert [t.input_prefix for t in cfg.event_tables]
