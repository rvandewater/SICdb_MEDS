"""The packaged suMEDS rules file loads in suMEDS and matches the codes this ETL writes.

suMEDS (https://github.com/rvandewater/SumMEDS) reads `src/SICdb_MEDS/sumeds_rules.yaml` to add
same-dataset ICD-10-GM parents to `metadata/codes.parquet`. The package registers the file in the
`sumeds.rules` entry-point group under its pipeline name, `SICdb`, and suMEDS's strict loader
(`sumeds.load_rules("SICdb")`) validates it. The hierarchy rule runs through suMEDS's own
enrichment against an empty local ontology source, so nothing is fetched. Every example code is rendered by MEDS-Extract from
the code expressions of `messy.yaml`, so a change to a code layout that breaks a rule fails here.
All values are made up or public ICD-10 codes.
"""

import json
from importlib.resources import files
from pathlib import Path

import polars as pl
import pytest
from MEDS_extract.config import MessyConfig
from sumeds import (
    EnrichmentConfig,
    OntologyRules,
    enrich_metadata,
    load_rules,
    registered_rules,
)
from sumeds.ontology import INDEX_SCHEMA

from SICdb_MEDS import MESSY_CFG, PIPELINE_NAME

RULES = files("SICdb_MEDS") / "sumeds_rules.yaml"

# Integer column values that pass the config's guards (first case, Sex 735/736, LaboratoryType
# 2296, the data_ref RefIDs and FieldIDs, ...), for rendering every other code layout.
VALUES = [0, 1, 735, 736, 738, 740, 2296, 3097, 3111, 3116]


@pytest.fixture(scope="module")
def cfg() -> MessyConfig:
    return MessyConfig.load(MESSY_CFG)


@pytest.fixture(scope="module")
def rules() -> OntologyRules:
    """The rules, loaded by the name this package registers them under."""
    return load_rules(PIPELINE_NAME)


@pytest.fixture
def empty_ontology_source(tmp_path) -> str:
    """A local ontology source without packs, so enrichment fetches nothing over the network."""
    (tmp_path / "index.json").write_text(
        json.dumps({"schema": INDEX_SCHEMA, "vocabularies": {}})
    )
    return str(tmp_path)


def diagnosis_codes(cfg: MessyConfig, values: list[str | None]) -> list[str | None]:
    """The codes the `cases/diagnosis` event emits for these `ICD10Main` values."""
    cases = next(t for t in cfg.event_tables if t.input_prefix == "cases")
    event = next(e for e in cases.events if e.name == "diagnosis")
    frame = pl.LazyFrame({"ICD10Main": values}, schema={"ICD10Main": pl.String})
    return frame.select(event.polars_exprs["code"]).collect().to_series().to_list()


def other_codes(cfg: MessyConfig) -> set[str]:
    """Codes of every other event, rendered from rows of `VALUES` (aligned and rotated)."""
    codes = set()
    for table in cfg.event_tables:
        columns = sorted(
            (table.source_columns() | set(table.joined_columns))
            - set(table.col_outputs)
        )
        n = len(VALUES)
        rows = {
            c: VALUES + [VALUES[(i + k) % n] for i in range(n)]
            for k, c in enumerate(columns)
        }
        prepared = table.prepare(pl.LazyFrame(rows))
        for event in table.events:
            if (table.input_prefix, event.name) == ("cases", "diagnosis"):
                continue
            rendered = prepared.select(event.polars_exprs["code"]).collect().to_series()
            codes.update(rendered.drop_nulls().to_list())
    return codes


def test_rules_are_package_data():
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text()
    assert '"sumeds_rules.yaml"' in pyproject
    assert RULES.is_file()


def test_rules_are_registered_under_the_pipeline_name():
    """The spec's `sumeds_enrich` stage and `--rules SICdb` find the rules by the pipeline's name."""
    assert PIPELINE_NAME in registered_rules()
    assert registered_rules()[PIPELINE_NAME] == "SICdb_MEDS:sumeds_rules.yaml"
    by_path = load_rules(Path(str(RULES)))
    assert [rule.id for rule in by_path.hierarchy] == [
        rule.id for rule in load_rules(PIPELINE_NAME).hierarchy
    ]


def test_rules_load_in_sumeds(rules: OntologyRules):
    """The strict suMEDS loader accepts every rule."""
    assert not rules.ontology
    assert not rules.crosswalk
    assert [rule.id for rule in rules.hierarchy] == ["sicdb-diagnosis-icd10gm-prefix"]


@pytest.mark.parametrize(
    "values, expected",
    [
        (
            ["I10", "E11", "E11.9", "S62", "S62.6", "S62.60", "C19.x1", "103.2"],
            {"E11.9": "E11", "S62.6": "S62", "S62.60": "S62.6"},
        ),
        # Without `S62.6` in the data, `S62.60` gets the next shorter code that occurs.
        (["S62", "S62.60"], {"S62.60": "S62"}),
        # Non-canonical values are no one's parent either.
        (["C19", "C19.x1", "103", "103.2"], {}),
    ],
)
def test_diagnosis_hierarchy_rule(cfg, rules, values, expected, empty_ontology_source):
    """Run suMEDS's own enrichment, offline, with the hierarchy rule over diagnosis codes."""
    enriched = enrich_metadata(
        pl.LazyFrame({"code": diagnosis_codes(cfg, values)}),
        EnrichmentConfig(rules=rules, ontology_source=empty_ontology_source),
    ).collect()
    parents = {
        row["code"]: row["parent_codes"]
        for row in enriched.select("code", "parent_codes").to_dicts()
        if row["parent_codes"]
    }
    code = dict(zip(values, diagnosis_codes(cfg, values)))
    assert parents == {
        code[child]: [code[parent]] for child, parent in expected.items()
    }


def test_rules_match_no_other_code_layout(cfg, rules):
    codes = other_codes(cfg)
    families = {code.split("//")[0] for code in codes}
    # The rendering reaches every family of the config but the diagnosis itself.
    assert {
        "LAB",
        "MEDICATION",
        "MEDICATION_RATE",
        "CASE_ATTR",
        "VITAL",
        "MEDS_BIRTH",
    } <= families
    for rule in rules.hierarchy:
        assert [code for code in codes if rule.candidates(code)] == [], rule.id
