# SICdb_MEDS ETL

[![PyPI - Version](https://img.shields.io/pypi/v/SICdb_MEDS)](https://pypi.org/project/SICdb_MEDS/)
[![Documentation Status](https://readthedocs.org/projects/SICdb_MEDS/badge/?version=latest)](https://SICdb_MEDS.readthedocs.io/en/latest/?badge=latest)
[![codecov](https://codecov.io/gh/rvandewater/SICdb_MEDS/graph/badge.svg?token=E7H6HKZV3O)](https://codecov.io/gh/rvandewater/SICdb_MEDS)
[![tests](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/tests.yaml/badge.svg)](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/tests.yml)
[![code-quality](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/code-quality-main.yaml/badge.svg)](https://github.com/rvandewater/SICdb_MEDS/actions/workflows/code-quality-main.yaml)
![python](https://img.shields.io/badge/-Python_3.12-blue?logo=python&logoColor=white)
![Static Badge](https://img.shields.io/badge/MEDS-0.4.1-blue)
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
pip install SICdb_MEDS
export DATASET_DOWNLOAD_USERNAME=$PHYSIONET_USERNAME
export DATASET_DOWNLOAD_PASSWORD=$PHYSIONET_PASSWORD

suMEDS-extract spec=SICdb output_dir=$MEDS_COHORT_DIR

# or, if the release is already on disk
suMEDS-extract spec=SICdb output_dir=$MEDS_COHORT_DIR do_download=false input_dir=$RAW_INPUT_DIR
```

[suMEDS](https://github.com/rvandewater/SumMEDS)' `suMEDS-extract` (installed with this package)
runs the ETL with `meds-extract-run spec=SICdb`, then adds LOINC names and same-dataset diagnosis
parents to `metadata/codes.parquet` (see
[Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds)). Arguments before a
`--` go to `meds-extract-run`, and arguments after it go to `suMEDS-enrich`. For the plain ETL, run
`meds-extract-run spec=SICdb output_dir=...` with the same arguments.

Waveform extraction is not currently supported; see
[#1](https://github.com/rvandewater/SICdb_MEDS/issues/1).

## Configuration

The entire ETL is described by one file, `src/SICdb_MEDS/messy.yaml` — a
[MESSY](https://github.com/mmcdermott/MEDS_extract) config carrying three sections:

- **`sources:`** — where the raw data lives. `meds-extract-download` stages it, with SHA-256
    verification and resumable transfers. This replaces the old hand-rolled `download.py`.
- **`etl:`** — the dataset name plus curated stage options (`n_subjects_per_shard`).
- **the event tables** — what to extract, written in
    [dftly](https://github.com/mmcdermott/dftly) expressions.

Because the config is registered under the `MEDS_extract.pipelines` entry-point group, the
extraction half is runnable directly, without this package's CLI wrapper:

```bash
# Stage the raw data only:
meds-extract-download spec=SICdb output_dir=$RAW_INPUT_DIR

# Run the canonical 8-stage pipeline over already-pre-MEDS'd data:
meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR download_key=null input_dir=$PRE_MEDS_DIR
```

There is no wrapper CLI and no pre-MEDS step. Both are gone: the archive is unpacked by the
download layer, and offsets are resolved to absolute pseudo-timestamps in `_table.cols`. The
package ships no Python at all -- the whole ETL is `src/SICdb_MEDS/messy.yaml`. A second entry point
registers [`src/SICdb_MEDS/sumeds_rules.yaml`](src/SICdb_MEDS/sumeds_rules.yaml) with suMEDS under
the same name, which is all `suMEDS-extract spec=SICdb` needs to enrich the output.

## Code metadata

`metadata/codes.parquet` describes every code in the data. Every description is defined in
`messy.yaml` and comes from SICdb's own tables or its
[documentation](https://www.sicdb.com/Documentation/Main_Page):

- Codes that carry a `d_references` id (`ADMISSION//…`, `DISCHARGE//…`, `LAB//…`, `MEDICATION//…`,
    `CASE_ATTR//…`, `DEVICE//…`, `VITAL//…` and the other `cases` reference columns): the
    `d_references` entry, through a `_metadata` block keyed on the id. Lab codes also get the unit and
    the LOINC long name.
- `DIAGNOSIS//ICD10GM//…`: `cases.ICD10MainText`.
- Every other code (`MEDS_BIRTH`, `MEDS_DEATH`, `SEX//…`, `ADMISSION_YEAR//…`, `ICU_ADMISSION`,
    `WEIGHT`, `SAPS3`, `HEART_SURGERY_…`, …): a `_self` block whose description names the source
    column.

MEDS-Extract 0.8 rejects a `_self` block on a bare literal code (`code: ICU_ADMISSION`). The literal
codes are therefore read from constant columns of `cases._table.cols`
(`_code_icu_admission: '"ICU_ADMISSION"'`), and the emitted codes are unchanged. The next MEDS-Extract
release accepts `_self` on a literal code
([MEDS_extract#315](https://github.com/mmcdermott/MEDS_extract/pull/315)).

The ETL writes the vocabulary parents that SICdb records itself:

- `ICD10GM/{ICD10Main}` for a diagnosis with a canonical ICD-10-GM code.
- `LOINC/{LOINC_code}` for a lab whose `d_references.LOINC_code` is a well-formed LOINC code.

## Enriching code metadata with suMEDS

Vocabulary work lives in [suMEDS](https://github.com/rvandewater/SumMEDS) rather than in this ETL.
The package ships the rules for it,
[`src/SICdb_MEDS/sumeds_rules.yaml`](src/SICdb_MEDS/sumeds_rules.yaml), registered in suMEDS's
`sumeds.rules` entry-point group under the same name as the MEDS-Extract spec, `SICdb`. That is how
`suMEDS-extract spec=SICdb` finds them, and `--rules SICdb` works for any suMEDS command.
`suMEDS-extract` replaces `metadata/codes.parquet` only once the enriched table is complete, and it
stays valid MEDS code metadata. suMEDS options go after a `--`:

```bash
# Also look codes up in your own OHDSI Athena download (the only source for ICD-10-GM):
suMEDS-extract spec=SICdb output_dir=$MEDS_COHORT_DIR -- --athena-csv /path/to/athena
# Use only already cached packs (no network):
suMEDS-extract spec=SICdb output_dir=$MEDS_COHORT_DIR -- --offline
```

Running it again on an existing output skips the pipeline stages that are already done and
enriches again. To enrich the `codes.parquet` of an existing build instead:

```bash
suMEDS-enrich "$MEDS_COHORT_DIR/metadata/codes.parquet" -o "$MEDS_COHORT_DIR/metadata/codes.parquet" --rules SICdb
```

The ETL already describes every code, and suMEDS never overwrites a value, so the descriptions stay
SICdb's own. For SICdb, suMEDS adds:

- **LOINC names.** The `LOINC/…` parents of `LAB//…` codes are looked up in suMEDS's LOINC pack,
    fetched from GitHub on first use and cached; no Athena download is needed. Each gets its LOINC
    long common name in `parent_names` (a map from parent code to name), and the lab code gets
    `vocabulary_id` `LOINC` and its `concept_code`. `--loinc-hierarchy` also adds the LOINC Parts
    above the term as parents.
- **ICD-10-GM names**, with `--athena-csv` only. suMEDS has no ICD-10-GM pack, so the
    `ICD10GM/…` parents of `DIAGNOSIS//ICD10GM//…` codes are resolved only from an Athena download
    that includes the vocabulary, which also gives the diagnosis codes their OMOP concept fields.
- **Same-dataset diagnosis parents**, from the one rule of the file:

| Rule                             | Kind                                     | Adds                                                                                                                                                            |
| -------------------------------- | ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `sicdb-diagnosis-icd10gm-prefix` | hierarchy, `mode: prefix`, `ignore: "."` | the nearest shorter diagnosis code that also occurs in the data (`DIAGNOSIS//ICD10GM//S62.60` gets `DIAGNOSIS//ICD10GM//S62.6`, else `DIAGNOSIS//ICD10GM//S62`) |

Only canonical ICD-10-GM codes take part, the same shapes the ETL's `ICD10GM/` parent accepts. The
parents the ETL wrote keep their place at the start of `parent_codes`, and the added ones follow. The
same rules work with `suMEDS … --rules SICdb` to build a privacy-masked code catalog of the output.

`tests/test_sumeds_rules.py` loads the rules by their registered name and runs the rule, offline, on
codes that MEDS-Extract renders from `messy.yaml`. `tests/test_run_with_enrichment.py` runs
`suMEDS-extract spec=SICdb` on a small synthetic release (`tests/conftest.py`) with a local LOINC
pack, and checks that the enrichment keeps every value the ETL wrote.

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

## Summary statistics

[`tests/regression/full_summary_stats.json`](tests/regression/full_summary_stats.json) is a
[meds_summary_stats](https://github.com/Medical-Event-Data-Standard/meds_summary_stats) fingerprint of
the MEDS dataset this ETL builds from the full SICdb 1.0.8 release with
`suMEDS-extract spec=SICdb`, so its code-metadata coverage includes the suMEDS enrichment. It covers
subject, event and measurement counts, digests of the code vocabulary, per-subject, value and time
distributions, code-metadata coverage, the splits and the schema. It names no code and reports no
cell of fewer than 20 subjects and no minimum or maximum, which is what makes it safe to publish.
Because it is committed next to the ETL, its diff shows what an ETL change did to the output, and two
ETL versions can be compared without access to the data.

Regenerate it in every pull request that changes the output, from a build of that branch:

```bash
# Build the release with this checkout, then summarize it
scripts/summary_stats.sh full --input-dir $RAW_INPUT_DIR
# Or summarize a build of this checkout you already have
scripts/summary_stats.sh full --meds-dir $MEDS_OUTPUT_DIR
```

Both print how the statistics changed against the committed baseline before overwriting it. Add
`--compare` to check a build without writing anything; it exits 1 when
[`tests/regression/policy.yaml`](tests/regression/policy.yaml) flags an error. Rebuilding the same
commit reproduces the baseline exactly, so any finding is a change in the output. CI cannot rebuild the
credentialed release, so the [Summary stats workflow](.github/workflows/summary-stats.yaml) only warns
when a pull request changes `src/` (the config or the suMEDS rules) without regenerating the
baseline. A MEDS-Extract, suMEDS or polars upgrade can also move the output, so check it with
`--compare`.

## Development

See [DEVELOPMENT.md](DEVELOPMENT.md) for setup, tests, running the ETL locally, and the release
process.

## Citation

If you use this dataset, please cite the original publication below and the ETL (see cite this repository):

```bibtex

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

For the ETL:

```bibtex
@software{van_de_Water_SICdb_MEDS_ETL_2025,
author = {van de Water, Robin P.},
doi = {10.5281/zenodo.14893939},
license = {MIT},
month = feb,
title = {{SICdb\_MEDS ETL}},
url = {https://github.com/rvandewater/SICdb_MEDS},
version = {0.0.1},
year = {2025}
}
```
