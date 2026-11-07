"""A tiny, fully synthetic SICdb-shaped release and one ETL run over it, shared by the tests.

SICdb is credentialed-access (no open demo), so CI cannot download it. Instead this writes a
handful of made-up rows with the columns `messy.yaml` reads from each table (the layout of the
SICdb documentation, https://www.sicdb.com/Documentation/Main_Page) and runs
``meds-extract-run`` on them with ``do_download=false``. No values here come from the real
dataset; the reference ids, codes and texts are invented.

The rows exercise what the code metadata depends on:

- diagnoses `S62`, `S62.6` and `S62.60` (a prefix chain), `E11.9` (whose shorter code does not
  occur) and the non-canonical `C19.x1`, which gets no `ICD10GM/` parent;
- one analyte with a canonical LOINC code, one whose `LOINC_code` is a local id (`ZL-315`) and
  one without, plus an arterial blood-gas measurement of the first;
- the guards of the config: a 0 weight and height, an Amount of 0, a case without a hospital
  stay length, and the `data_ref` FieldID 3096 placeholder.
"""

import csv
import subprocess
from pathlib import Path

import polars as pl
import pytest

# Patients 1-4. MEDS-Extract needs >= 1 per train/tuning/held_out split.
#   Case 101: patient 1, first admission.
#   Case 102: patient 1, readmitted a year later; died 10 days after that admission.
#   Case 201: patient 2, heart surgery; weight and height 0 (the missing-value placeholder).
#   Case 301: patient 3.
#   Case 401: patient 4, non-canonical diagnosis code, no hospital stay length.
CASE_DEFAULTS = {
    "OffsetAfterFirstAdmission": 0,
    "AdmissionYear": 2015,
    "AgeOnAdmission": 60,
    "Sex": 735,
    "WeightOnAdmission": 80000,
    "HeightOnAdmission": 180,
    "ICUOffset": 3600,
    "TimeOfStay": 259200,
    "HospitalDischargeDay": 7,
    "HospitalStayDays": 7,
    "OffsetOfDeath": "",
    "EstimatedSurvivalObservationTime": "",
    "AdmissionUrgency": 1001,
    "ReferringUnit": 1011,
    "SurgicalAdmissionType": 1021,
    "SurgicalSite": 1031,
    "AdmissionFormHasSepsis": 738,
    "DischargeState": 1051,
    "DischargeUnit": 1061,
    "HospitalUnit": 1071,
    "HospitalDischargeType": 1081,
    "saps3": 45,
    "HoursOfCRRT": 0,
    "DiagnosisT2": "",
    "HeartSurgeryBeginOffset": "",
    "HeartSurgeryEndOffset": "",
    "HeartSurgeryCPBTime": "",
    "HeartSurgeryCrossClampTime": "",
}

CASES = [
    {
        "CaseID": 101,
        "PatientID": 1,
        "ICD10Main": "S62",
        "ICD10MainText": "Synthetic diagnosis S62",
        "DiagnosisT2": "Synthetic secondary diagnosis",
    },
    {
        "CaseID": 102,
        "PatientID": 1,
        "OffsetAfterFirstAdmission": 31557600,
        "AdmissionYear": 2016,
        "ICD10Main": "S62.60",
        "ICD10MainText": "Synthetic diagnosis S62.60",
        "OffsetOfDeath": 864000,
        "EstimatedSurvivalObservationTime": 1041,
        "AdmissionFormHasSepsis": 740,
        "HoursOfCRRT": 12,
    },
    {
        "CaseID": 201,
        "PatientID": 2,
        "Sex": 736,
        "WeightOnAdmission": 0,
        "HeightOnAdmission": 0,
        "ICD10Main": "S62.6",
        "ICD10MainText": "Synthetic diagnosis S62.6",
        "HeartSurgeryBeginOffset": 1800,
        "HeartSurgeryEndOffset": 14400,
        "HeartSurgeryCPBTime": 90,
        "HeartSurgeryCrossClampTime": 60,
    },
    {
        "CaseID": 301,
        "PatientID": 3,
        "Sex": 736,
        "AgeOnAdmission": 75,
        "ICD10Main": "E11.9",
        "ICD10MainText": "Synthetic diagnosis E11.9",
    },
    {
        "CaseID": 401,
        "PatientID": 4,
        "AdmissionYear": 2019,
        "ICD10Main": "C19.x1",
        "ICD10MainText": "Synthetic diagnosis C19.x1",
        "HospitalStayDays": "",
    },
]

CASE_COLUMNS = ["CaseID", "PatientID", "ICD10Main", "ICD10MainText", *CASE_DEFAULTS]

# ReferenceGlobalID -> (ReferenceValue, ReferenceName, ReferenceUnit, LOINC_code, LOINC_long).
D_REFERENCES = {
    735: ("Male", "Sex", "", "", ""),
    736: ("Female", "Sex", "", "", ""),
    738: ("No", "YesNoUnknown", "", "", ""),
    740: ("Yes", "YesNoUnknown", "", "", ""),
    1001: ("Synthetic urgency", "AdmissionUrgency", "", "", ""),
    1011: ("Synthetic referring unit", "ReferringUnit", "", "", ""),
    1021: ("Synthetic surgical admission", "SurgicalAdmissionType", "", "", ""),
    1031: ("Synthetic surgical site", "SurgicalSite", "", "", ""),
    1041: ("Synthetic observation window", "SurvivalObservationTime", "", "", ""),
    1051: ("Synthetic discharge state", "DischargeState", "", "", ""),
    1061: ("Synthetic discharge unit", "DischargeUnit", "", "", ""),
    1071: ("Synthetic hospital unit", "HospitalUnit", "", "", ""),
    1081: ("Synthetic hospital discharge", "HospitalDischargeType", "", "", ""),
    2001: (
        "Synthetic analyte A",
        "Laboratory",
        "mg/dl",
        "2160-0",
        "Synthetic LOINC long A",
    ),
    2002: ("Synthetic analyte B", "Laboratory", "mmol/l", "ZL-315", ""),
    2003: ("Synthetic analyte C", "Laboratory", "U/l", "", ""),
    2296: ("Arterial", "LaboratoryType", "", "", ""),
    3001: ("Synthetic drug", "Drug", "mg", "", ""),
    3002: ("Synthetic fluid", "Fluid", "ml", "", ""),
    3096: ("Synthetic heart surgery type", "Field", "", "", ""),
    3097: ("Synthetic precondition", "Field", "", "", ""),
    3111: ("Synthetic NYHA class", "Field", "", "", ""),
    3120: ("Synthetic AKI stage", "Field", "", "", ""),
    4001: ("Synthetic arterial line", "Device", "", "", ""),
    5001: ("Synthetic heart rate", "SignalFloat", "bpm", "", ""),
}

LABORATORY = [
    # id, CaseID, LaboratoryID, Offset, LaboratoryValue, LaboratoryType
    (1, 101, 2001, 7200, 1.1, ""),
    (2, 101, 2001, 9000, 1.3, 2296),
    (3, 102, 2002, -3600, 4.2, ""),
    (4, 201, 2003, 3600, 140.0, ""),
    (5, 301, 2001, 3600, 0.9, ""),
    (6, 401, 2002, 3600, 3.9, ""),
]

MEDICATION = [
    # id, PatientID, CaseID, DrugID, Offset, OffsetDrugEnd, Amount, AmountPerMinute, IsSingleDose
    (1, 1, 101, 3001, 3600, 3660, 5.0, 5.0, 1),
    (2, 1, 101, 3002, 7200, 14400, 500.0, 1.39, 0),
    (3, 2, 201, 3001, 1800, 1860, 0.0, 0.0, 1),
    (4, 3, 301, 3002, 3600, 7200, 250.0, 2.08, 0),
]

DATA_REF = [
    # id, CaseID, FieldID, RefID
    (1, 101, 3097, 740),
    (2, 101, 3111, 2324),
    (3, 102, 3120, 3116),
    (4, 201, 3096, 2017),
]

DATA_RANGE = [
    # id, CaseID, DataID, Offset, OffsetEnd
    (1, 101, 4001, 3600, 86400),
    (2, 201, 4001, 1800, 50000),
]

DATA_FLOAT_H = [
    # id, CaseID, DataID, Offset, Val, cnt
    (1, 101, 5001, 3600, 80.5, 60),
    (2, 102, 5001, 3600, 92.0, 58),
    (3, 301, 5001, 7200, 70.0, 60),
    (4, 401, 5001, 3600, 85.0, 60),
]

TABLES = {
    "cases": (
        CASE_COLUMNS,
        [[{**CASE_DEFAULTS, **case}[c] for c in CASE_COLUMNS] for case in CASES],
    ),
    "d_references": (
        [
            "ReferenceGlobalID",
            "ReferenceValue",
            "ReferenceName",
            "ReferenceUnit",
            "LOINC_code",
            "LOINC_long",
        ],
        [[ref_id, *values] for ref_id, values in D_REFERENCES.items()],
    ),
    "laboratory": (
        ["id", "CaseID", "LaboratoryID", "Offset", "LaboratoryValue", "LaboratoryType"],
        LABORATORY,
    ),
    "medication": (
        [
            "id",
            "PatientID",
            "CaseID",
            "DrugID",
            "Offset",
            "OffsetDrugEnd",
            "Amount",
            "AmountPerMinute",
            "IsSingleDose",
        ],
        MEDICATION,
    ),
    "data_ref": (["id", "CaseID", "FieldID", "RefID"], DATA_REF),
    "data_range": (["id", "CaseID", "DataID", "Offset", "OffsetEnd"], DATA_RANGE),
    "data_float_h": (["id", "CaseID", "DataID", "Offset", "Val", "cnt"], DATA_FLOAT_H),
}


@pytest.fixture(scope="session")
def raw_dir(tmp_path_factory) -> Path:
    """The synthetic tables, written as the raw SICdb CSVs."""
    raw_dir = tmp_path_factory.mktemp("sicdb_raw")
    for name, (columns, rows) in TABLES.items():
        with (raw_dir / f"{name}.csv").open("w", newline="") as f:
            writer = csv.writer(f, lineterminator="\n")
            writer.writerows([columns, *rows])
    return raw_dir


def run_command(argv: list[str]) -> str:
    """Run a CLI, failing the test with its full output if it does not succeed; return its stderr."""
    result = subprocess.run(argv, capture_output=True, text=True)
    assert result.returncode == 0, (
        f"{argv[0]} failed with exit code {result.returncode}.\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    return result.stderr


@pytest.fixture(scope="session")
def meds_output(tmp_path_factory, raw_dir: Path) -> Path:
    """The plain (unenriched) ETL over the synthetic release."""
    output_dir = tmp_path_factory.mktemp("sicdb") / "MEDS_cohort"
    run_command(
        [
            "meds-extract-run",
            "spec=SICdb",
            f"output_dir={output_dir}",
            "do_download=false",
            f"input_dir={raw_dir}",
        ]
    )
    return output_dir


@pytest.fixture(scope="session")
def events(meds_output: Path) -> pl.DataFrame:
    return pl.concat(
        [pl.read_parquet(fp) for fp in (meds_output / "data").rglob("*.parquet")]
    )


@pytest.fixture(scope="session")
def codes(meds_output: Path) -> pl.DataFrame:
    return pl.read_parquet(meds_output / "metadata" / "codes.parquet")
