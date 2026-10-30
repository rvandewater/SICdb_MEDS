# SICdb_MEDS ETL

[![PyPI - Version](https://img.shields.io/pypi/v/SICdb_MEDS)](https://pypi.org/project/SICdb_MEDS/)
[![Documentation Status](https://readthedocs.org/projects/SICdb_MEDS/badge/?version=latest)](https://SICdb_MEDS.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/rvandewater/SICdb_MEDS/graph/badge.svg?token=E7H6HKZV3O)](https://codecov.io/gh/rvandewater/SICdb_MEDS)
[![tests](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.12-blue?logo=python&logoColor=white)
![Static Badge](https://img.shields.io/badge/MEDS-0.3.3-blue)
[![license](https://img.shields.io/badge/License-MIT-green.svg?labelColor=gray)](https://github.com/rvandewater/SICdb_MEDS#license)
[![PRs](https://img.shields.io/badge/PRs-welcome-brightgreen.svg)](https://github.com/rvandewater/SICdb_MEDS/pulls)
[![contributors](https://img.shields.io/github/contributors/rvandewater/SICdb_MEDS.svg)](https://github.com/rvandewater/SICdb_MEDS/graphs/contributors)
[![DOI](https://zenodo.org/badge/932832366.svg)](https://doi.org/10.5281/zenodo.14893938)

The SICdb dataset offers insights into over 27 thousand intensive care admissions, including therapies and data on
preceding surgeries. Data were collected between 2013 and 2021 from four different intensive care units at the
University Hospital Salzburg, having more than 3 thousand intensive care admissions per year on 41 beds. The dataset is
deidentified and contains, amongst others, case information, vital signs, laboratory results and medication data. SICdb
provides both aggregated once-per-hour and highly granular once-per-minute data, making it suitable for computational
and machine learning-based research. (source: https://www.sicdb.com/Documentation/Main_Page)

## Usage

```bash
pip install SICdb_MEDS # you can do this locally or via PyPI
# Download your data or set download credentials
MEDS_extract-SICdb root_output_dir=$ROOT_OUTPUT_DIR

# or, if you have the data already downloaded
MEDS_extract-SICdb root_output_dir=$ROOT_OUTPUT_DIR do_download=False

# or, if you want enable waveform extraction and processing (takes significantly longer and up to 100GB of RAM)
MEDS_extract-SICdb root_output_dir=$ROOT_OUTPUT_DIR do_process_waveform=True
```

## MEDS-transforms settings

If you want to convert a large dataset, you can use parallelization with MEDS-transforms
(the MEDS-transformation step that takes the longest).

Using local parallelization with the `hydra-joblib-launcher` package, you can set the number of workers:

```
pip install hydra-joblib-launcher --upgrade
```

Then, you can set the number of workers as environment variable:

```bash
export N_WORKERS=8
```

Moreover, you can set the number of subjects per shard to balance the parallelization overhead based on how many
subjects you have in your dataset:

```bash
export N_SUBJECTS_PER_SHARD=100000
```

## Code metadata

`metadata/codes.parquet` lists every code in the data with a `description`. The ETL adds no
vocabulary parents (`parent_codes` is null); suMEDS adds them from the packaged rules file (see
[Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds)).

- `DIAGNOSIS//ICD10//{ICD10Main}`: SICdb's own `cases.ICD10MainText`.
- `LAB//LOINC//{LOINC_code}//{unit}`: the analyte name (`d_references.ReferenceValue`) and unit, all
    analytes that share the code.
- `SEX//…`, `VITAL//…`, `MEDICATION//…`, `MEDICATION_END//…`: the `d_references` value the code carries
    and what the event is.
- Literal codes (`MEDS_BIRTH`, `ADMISSION`, `WEIGHT`, ...) and `DIAGNOSIS//ICD10//UNK`:
    [`static_code_descriptions.csv`](src/SICdb_MEDS/metadata/static_code_descriptions.csv), which records
    the source of every row.

Pre-MEDS builds the first three from SICdb's own dictionary at ETL time, as the tables
`diagnosis_metadata`, `laboratory_metadata` and `reference_metadata` next to the pre-MEDS tables, and the
`_metadata` blocks in `configs/event_configs.yaml` join them onto the codes in `extract_code_metadata`.
Literal codes have no component to join on, so after the pipeline
[`code_metadata.finalize_code_metadata`](src/SICdb_MEDS/code_metadata.py) fills them in and writes
`codes.parquet` with the MEDS code-metadata schema. Nothing from SICdb is shipped with this package.

- The LOINC codes are SICdb's own (`d_references.LOINC_code`, added in 1.0.8).
- `WEIGHT` is in grams, as stored in `cases.WeightOnAdmission`.
- The pre-MEDS laboratory table now carries the analyte name; a `pre_MEDS/laboratory.parquet` from an
    older version of this ETL is rejected, so delete it and re-run.
- Descriptions follow the [SICdb documentation](https://www.sicdb.com/Documentation/Main_Page) (CC BY 4.0).

## Enriching code metadata with suMEDS

Vocabulary work (ontology tagging, LOINC validation) lives in
[suMEDS](https://github.com/rvandewater/SumMEDS) and its versioned ontology data (sumeds-ontologies,
pinned release), which maintain one copy for all MEDS ETLs. This package ships a rules file for it,
[`src/SICdb_MEDS/sumeds_rules.yaml`](src/SICdb_MEDS/sumeds_rules.yaml) (package data, format
`sumeds-rules/0.1`). After the ETL, run:

```bash
uvx --from "git+https://github.com/rvandewater/SumMEDS" suMEDS-enrich \
    $MEDS_cohort_dir/metadata/codes.parquet -o codes-enriched.parquet \
    --rules src/SICdb_MEDS/sumeds_rules.yaml
```

| Rule                           | Kind                                     | Adds                                                                                                                                                      |
| ------------------------------ | ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sicdb-diagnosis-icd10`        | ontology, `ICD10`, `validate: none`      | `ICD10//{ICD10Main}` as parent, or the four-character subcategory of a five-character code (`S62.60` gets `ICD10//S62.6`); other shapes get none          |
| `sicdb-lab-loinc`              | ontology, `LOINC`, `validate: pack`      | `LOINC//{LOINC_code}` with `vocabulary_id`/`concept_code`, when SICdb's own LOINC code is in the LOINC pack; a code that is not in LOINC gets nothing     |
| `sicdb-diagnosis-icd10-prefix` | hierarchy, `mode: prefix`, `ignore: '.'` | the nearest shorter diagnosis code that also occurs in the data (`DIAGNOSIS//ICD10//S62.60` gets `DIAGNOSIS//ICD10//S62.6`, else `DIAGNOSIS//ICD10//S62`) |

On SICdb 1.0.8 the rules tag 2,153 of 2,169 diagnosis codes with `ICD10`, validate 380 of 401 lab
codes against LOINC, and give 224 diagnosis codes a same-dataset parent. The ETL alone does not emit
these parents.

SICdb does not name its ICD-10 version. Austrian hospitals code with the Austrian ICD-10, which the
health ministry describes as based on the German-language ICD-10 WHO, with some five-character codes
taken from ICD-10-GM
([Medizinische Dokumentation, section 2.1](https://www.sozialministerium.at/dam/jcr:2acd92ba-9b21-45c4-a4f6-d8345943e7b1/MEDIZINISCHEDOKUMENTATION2022.pdf)).
The `ICD10` (WHO) tags are therefore an inference, not a documented mapping. WHO ICD-10 may not be
redistributed, so suMEDS has no pack for it: the rule only tags codes, or validates them when your
OHDSI Athena download (`--athena-csv`) has the `ICD10` vocabulary. `tests/test_sumeds_rules.py`
checks that the file is well formed and that every rule matches the code layouts of
`configs/event_configs.yaml`.

## Citation

If you use this dataset, please cite the original publication below and the ETL (see cite this repository):

```

@article{rodemundHarnessingBigData2024,
title = {Harnessing {Big} {Data} in {Critical} {Care}: {Exploring} a new {European} {Dataset}},
volume = {11},
copyright = {2024 The Author(s)},
issn = {2052-4463},
shorttitle = {Harnessing {Big} {Data} in {Critical} {Care}},
url = {https://www.nature.com/articles/s41597-024-03164-9},
doi = {10.1038/s41597-024-03164-9},
language = {en},
number = {1},
urldate = {2024-04-04},
journal = {Scientific Data},
author = {Rodemund, Niklas and Wernly, Bernhard and Jung, Christian and Cozowicz, Crispiana and Koköfer, Andreas},
month = mar,
year = {2024},
note = {Publisher: Nature Publishing Group},
keywords = {Clinical trial design, Experimental models of disease},
pages = {320},
}

```
