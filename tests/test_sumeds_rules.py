"""The packaged suMEDS rules file is well formed and matches the codes this ETL writes.

suMEDS (https://github.com/rvandewater/SumMEDS) reads `src/SICdb_MEDS/sumeds_rules.yaml` to add
ICD-10 and LOINC parents to `metadata/codes.parquet`. These checks mirror its strict
`sumeds-rules/0.1` loader without depending on it, and build the example codes from the code
templates of `configs/event_configs.yaml` (components joined with `//`, a missing component filled
with `UNK` as MEDS-Transforms does), so a change to a code layout that breaks a rule fails here.
All values are made up or public codes (ICD-10 categories, LOINC 2160-0).
"""

import re
from importlib.resources import files
from pathlib import Path

import pytest
import yaml
from omegaconf import OmegaConf

RULES = files("SICdb_MEDS") / "sumeds_rules.yaml"
CONFIG_FP = (
    Path(__file__).parents[1] / "src" / "SICdb_MEDS" / "configs" / "event_configs.yaml"
)

TOP_KEYS = {"schema", "ontology", "crosswalk", "hierarchy"}
ONTOLOGY_KEYS = {"id", "pattern", "vocabulary", "normalize", "validate"}
CROSSWALK_KEYS = {"id", "pattern", "table", "key_column", "code_column", "vocabulary"}
CROSSWALK_KEYS |= {"where", "normalize", "validate"}
HIERARCHY_KEYS = {"id", "pattern", "group", "mode", "separator", "ignore"}
HIERARCHY_KEYS |= {"nearest_only", "from_ontology", "rules"}


def _rules() -> dict:
    return yaml.safe_load(RULES.read_text(encoding="utf-8"))


def _rule(rule_id: str) -> dict:
    rules = _rules()
    return next(
        r
        for section in ("ontology", "crosswalk", "hierarchy")
        for r in rules.get(section) or []
        if r["id"] == rule_id
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


def _vocabulary_code(rule_id: str, code: str) -> str | None:
    """The vocabulary code an ontology rule yields for ``code`` (its ``normalize`` applied)."""
    rule = _rule(rule_id)
    match = re.search(rule["pattern"], code)
    if match is None:
        return None
    value = match["code"]
    for step in rule.get("normalize") or []:
        value = re.sub(step["pattern"], step["replace"], value)
    return value


def test_rules_file_is_well_formed():
    rules = _rules()
    assert set(rules) <= TOP_KEYS
    assert rules["schema"] == "sumeds-rules/0.1"
    ids = []
    for rule in rules.get("ontology") or []:
        assert set(rule) <= ONTOLOGY_KEYS
        assert {"id", "pattern", "vocabulary"} <= set(rule)
        assert "code" in re.compile(rule["pattern"]).groupindex
        ids.append(rule["id"])
    for rule in rules.get("crosswalk") or []:
        assert set(rule) <= CROSSWALK_KEYS
        assert {"id", "pattern", "table", "key_column", "code_column"} <= set(rule)
        assert "key" in re.compile(rule["pattern"]).groupindex
        ids.append(rule["id"])
    for rule in (rules.get("ontology") or []) + (rules.get("crosswalk") or []):
        assert "/" not in rule["vocabulary"]
        assert rule.get("validate", "pack") in {"pack", "athena", "none"}
        for step in rule.get("normalize") or []:
            assert set(step) == {"pattern", "replace"}
            re.compile(step["pattern"])
    for rule in rules.get("hierarchy") or []:
        assert set(rule) <= HIERARCHY_KEYS
        if not rule.get("from_ontology"):
            assert rule["group"] in re.compile(rule["pattern"]).groupindex
            assert rule.get("mode") == "prefix" or rule.get("separator")
        ids.append(rule["id"])
    assert len(ids) == len(set(ids)), "rule ids must be unique"


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
    code = _code("cases", "ICD10Main", ICD10Main=icd10main)
    assert code.startswith("DIAGNOSIS//ICD10//")
    assert _vocabulary_code("sicdb-diagnosis-icd10", code) == expected


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
    assert _vocabulary_code("sicdb-lab-loinc", code) == expected


def test_diagnosis_hierarchy_rule():
    rule = _rule("sicdb-diagnosis-icd10-prefix")
    assert rule["mode"] == "prefix" and rule["ignore"] == "."
    for value in ("I10", "E11.9", "S62.60"):
        match = re.search(rule["pattern"], _code("cases", "ICD10Main", ICD10Main=value))
        assert match[rule["group"]] == value
    for value in (None, "X1.x"):
        code = _code("cases", "ICD10Main", ICD10Main=value)
        assert re.search(rule["pattern"], code) is None


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
    for rule in rules["ontology"] + rules["hierarchy"]:
        assert not any(re.search(rule["pattern"], code) for code in other), rule["id"]


def test_rules_load_in_sumeds():
    """When suMEDS with rules support is installed, its own strict loader accepts the file."""
    ontology = pytest.importorskip("sumeds.ontology")
    rules = ontology.load_rules(Path(str(RULES)))
    assert [rule.id for rule in rules.ontology] == [
        "sicdb-diagnosis-icd10",
        "sicdb-lab-loinc",
    ]
    assert [rule.id for rule in rules.hierarchy] == ["sicdb-diagnosis-icd10-prefix"]
