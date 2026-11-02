"""The packaged suMEDS rules file loads in suMEDS and matches the codes this ETL writes.

suMEDS (https://github.com/rvandewater/SumMEDS) reads `src/SICdb_MEDS/sumeds_rules.yaml` to add
ICD-10 and LOINC parents to `metadata/codes.parquet`. Its strict loader (`sumeds.load_rules`)
validates the file, codes are resolved with the rule objects its enrichment uses, and the hierarchy
rule runs through suMEDS's own enrichment against an empty local ontology source, so nothing is
fetched. These checks only state what SICdb's codes must resolve to. The example codes are built
from the code templates of `configs/event_configs.yaml` (components joined with `//`, a missing
component filled with `UNK` as MEDS-Transforms does), so a change to a code layout that breaks a
rule fails here. All values are made up or public codes (ICD-10 categories, LOINC 2160-0).
"""

import json
from importlib.resources import files
from pathlib import Path

import polars as pl
import pytest
from omegaconf import OmegaConf
from sumeds import EnrichmentConfig, OntologyRules, enrich_metadata, load_rules
from sumeds.ontology import INDEX_SCHEMA

RULES = files("SICdb_MEDS") / "sumeds_rules.yaml"
CONFIG_FP = (
    Path(__file__).parents[1] / "src" / "SICdb_MEDS" / "configs" / "event_configs.yaml"
)


def _rules() -> OntologyRules:
    return load_rules(Path(str(RULES)))


def _rule(rule_id: str):
    rules = _rules()
    return next(
        r for r in (*rules.vocabulary_rules, *rules.hierarchy) if r.id == rule_id
    )


def _code(table: str, event: str, **values: str | None) -> str:
    """The code ``table``/``event`` of the config yields for the given column values."""
    code = OmegaConf.to_container(OmegaConf.load(CONFIG_FP))[table][event]["code"]
    parts = [code] if isinstance(code, str) else code
    out = []
    for part in parts:
        if part.startswith("col("):
            value = values[part[4:-1]]
            out.append("UNK" if value is None else value)
        else:
            out.append(part)
    return "//".join(out)


def _diagnosis(value: str | None) -> str:
    return _code("cases", "ICD10Main", ICD10Main=value)


@pytest.fixture
def empty_ontology_source(tmp_path) -> str:
    """A local ontology source without packs, so enrichment fetches nothing over the network."""
    (tmp_path / "index.json").write_text(
        json.dumps({"schema": INDEX_SCHEMA, "vocabularies": {}})
    )
    return str(tmp_path)


def test_rules_load_in_sumeds():
    """The strict suMEDS loader accepts every rule."""
    rules = _rules()
    assert [rule.id for rule in rules.ontology] == [
        "sicdb-diagnosis-icd10",
        "sicdb-lab-loinc",
    ]
    assert not rules.crosswalk
    assert [rule.id for rule in rules.hierarchy] == ["sicdb-diagnosis-icd10-prefix"]


def test_rules_are_package_data():
    pyproject = (Path(__file__).parents[1] / "pyproject.toml").read_text()
    assert '"sumeds_rules.yaml"' in pyproject


@pytest.mark.parametrize(
    "icd10main, expected",
    [
        ("I10", "I10"),
        ("E11.9", "E11.9"),
        ("S62.60", "S62.6"),  # a five-character code is tagged as its subcategory
        ("X1.x", None),
        (None, None),  # DIAGNOSIS//ICD10//UNK
    ],
)
def test_diagnosis_rule(icd10main, expected):
    code = _diagnosis(icd10main)
    assert code.startswith("DIAGNOSIS//ICD10//")
    assert _rule("sicdb-diagnosis-icd10").resolve(code) == expected


@pytest.mark.parametrize(
    "loinc, unit, expected",
    [
        ("2160-0", "mg/dl", "2160-0"),
        ("2160-0", None, "2160-0"),
        ("2160-0", "/nl", "2160-0"),
        (None, "U/l", None),  # LAB//LOINC//UNK//U/l
        ("LOCAL-1", None, None),
    ],
)
def test_lab_rule(loinc, unit, expected):
    code = _code("laboratory", "lab", LOINC_code=loinc, ReferenceUnit=unit)
    assert code.startswith("LAB//LOINC//")
    assert _rule("sicdb-lab-loinc").resolve(code) == expected


@pytest.mark.parametrize(
    "values, expected",
    [
        (
            ["I10", "E11", "E11.9", "S62", "S62.6", "S62.60", "X1.x", None],
            {"E11.9": "E11", "S62.6": "S62", "S62.60": "S62.6"},
        ),
        # Without `S62.6` in the data, `S62.60` gets the next shorter code that occurs.
        (["S62", "S62.60"], {"S62.60": "S62"}),
    ],
)
def test_diagnosis_hierarchy_rule(values, expected, empty_ontology_source):
    """Run suMEDS's own enrichment, offline, with the hierarchy rule over the example codes."""
    rule = _rule("sicdb-diagnosis-icd10-prefix")
    enriched = enrich_metadata(
        pl.LazyFrame({"code": [_diagnosis(value) for value in values]}),
        EnrichmentConfig(
            rules=OntologyRules(hierarchy=(rule,)),
            ontology_source=empty_ontology_source,
        ),
    ).collect()
    parents = {
        row["code"]: row["parent_codes"]
        for row in enriched.select("code", "parent_codes").to_dicts()
        if row["parent_codes"]
    }
    assert parents == {
        _diagnosis(code): [_diagnosis(parent)] for code, parent in expected.items()
    }


def test_rules_do_not_match_other_codes():
    other = [
        _code("cases", "sex", Sex="735"),
        _code(
            "data_float_h", "vital", ReferenceValue="Heart rate", ReferenceUnit="bpm"
        ),
        _code("medication", "medication", ReferenceName="Drug group"),
        _code(
            "medication",
            "medication_stop",
            ReferenceName="Drug group",
            ReferenceValue="Drug X",
            ReferenceUnit=None,
        ),
        "MEDS_BIRTH",
        "WEIGHT",
    ]
    rules = _rules()
    for rule in rules.ontology:
        assert all(rule.resolve(code) is None for code in other), rule.id
    for rule in rules.hierarchy:
        assert all(rule.candidates(code) == [] for code in other), rule.id
