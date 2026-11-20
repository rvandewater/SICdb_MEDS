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

meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR

# or, if the release is already on disk
meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR do_download=false input_dir=$RAW_INPUT_DIR
```

This one command runs the ETL and then enriches its code metadata with
[suMEDS](https://github.com/rvandewater/SumMEDS) (installed with this package): the spec ends with
suMEDS's `sumeds_enrich` stage, which adds LOINC names and same-dataset diagnosis parents to
`metadata/codes.parquet`. If it cannot (no internet, say), the ETL still succeeds, unenriched, and
`suMEDS-enrich $MEDS_COHORT_DIR` finishes it later. Add `skip_stages=[sumeds_enrich]` for the plain
ETL. See [Enriching code metadata with suMEDS](#enriching-code-metadata-with-sumeds).

Waveform extraction is not currently supported; see
[#1](https://github.com/rvandewater/SICdb_MEDS/issues/1).

## Configuration

The entire ETL is described by one file, `src/SICdb_MEDS/messy.yaml` — a
[MESSY](https://github.com/mmcdermott/MEDS_extract) config carrying three sections:

- **`sources:`** — where the raw data lives. `meds-extract-download` stages it, with SHA-256
    verification and resumable transfers. This replaces the old hand-rolled `download.py`.
- **`etl:`** — the dataset name plus curated stage options (`n_subjects_per_shard`), what
    `metadata/dataset.json` says about the dataset, and the suMEDS enrichment stage that runs after
    the ETL.
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
the same name, which is how the spec's enrichment stage finds them.

`metadata/dataset.json` gives the release's `license` and PhysioNet page (`description_uri`), and
lists `icustay_id`, the ICU admission key (`cases.CaseID`), under `raw_source_id_columns`.
MEDS-Extract lists every other extension column (`n_samples`, ...) under `other_extension_columns`.
`cases` has "a single entry for each intensive care admission"
([PhysioNet SICdb 1.0.8](https://physionet.org/content/sicdb/1.0.8/)), and the release has no
hospital-stay id, so the column takes the name the MIMIC-IV ETL and the other MEDS ETLs give the ICU
stay ([alignEHR#55](https://github.com/rvandewater/alignEHR/issues/55)). A case's timeline starts at
its first admission to a MetaVision ward, which can be a preceding surgery, so a few events of a case
fall before the ICU admission itself.

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

**Units.** `unit` is a String: the unit of the code's numeric values, one per code, written in
[UCUM](https://ucum.org/) by suMEDS's `sumeds_normalize_units` stage (`mg/dl` becomes `mg/dL`, `bpm`
becomes `{beats}/min`). The spelling as recorded stays in `unit_source_value`, a spelling suMEDS does
not know is kept as it is, and the codes keep their names. Codes with a `d_references` id (`LAB//…`,
`VITAL//…`, `MEDICATION//…`, `MEDICATION_RATE//…`, `MEDICATION_END//…`) take it from `ReferenceUnit`,
one row per id, so one unit per code. The six numeric codes described only in `messy.yaml` carry the
unit their description states, in UCUM: `WEIGHT` `kg`, `HEIGHT` `cm`, `CRRT_HOURS` `h`,
`HOSPITAL_STAY_DAYS` `d`, and `HEART_SURGERY_CPB_MINUTES` and `HEART_SURGERY_CROSSCLAMP_MINUTES`
`min`. `SAPS3` is a score and has none. The `d_references` descriptions state no units (their bracketed parts are never the
item's `ReferenceUnit`), so no unit is taken from them. MEDS-Extract 0.9 and later write `unit` as one
String per code and fail the build if a code has more than one unit
([MEDS_extract#336](https://github.com/mmcdermott/MEDS_extract/pull/336)), so the ETL needs no code
of its own for it.

MEDS-Extract 0.8.0 rejects a `_self` block on a bare literal code (`code: ICU_ADMISSION`). The literal
codes are therefore read from constant columns of `cases._table.cols`
(`_code_icu_admission: '"ICU_ADMISSION"'`), and the emitted codes are unchanged. MEDS-Extract 0.8.1
accepts `_self` on a literal code
([MEDS_extract#315](https://github.com/mmcdermott/MEDS_extract/pull/315)), so these can become literals
again.

The ETL writes the vocabulary parents that SICdb records itself:

- `ICD10GM/{ICD10Main}` for a diagnosis with a canonical ICD-10-GM code.
- `LOINC/{LOINC_code}` for a lab whose `d_references.LOINC_code` is a well-formed LOINC code.

SICdb records no LOINC code for its monitor signals or for weight and height, so the config adds
these parents itself. Each one comes from a public source:

| Code                       | LOINC parent    | Measures              | DataID source                             |
| -------------------------- | --------------- | --------------------- | ----------------------------------------- |
| `VITAL//708`               | `LOINC/8867-4`  | heart rate            | WeavEHR `heart_rate.yml`                  |
| `VITAL//719`               | `LOINC/9279-1`  | respiratory rate      | WeavEHR `respiratory_rate.yml`            |
| `VITAL//701`, `VITAL//704` | `LOINC/8480-6`  | systolic pressure     | WeavEHR `systolic_blood_pressure.yml`     |
| `VITAL//702`, `VITAL//705` | `LOINC/8462-4`  | diastolic pressure    | WeavEHR `diastolic_blood_pressure.yml`    |
| `VITAL//703`, `VITAL//706` | `LOINC/8478-0`  | mean pressure         | WeavEHR `mean_arterial_pressure.yml`      |
| `VITAL//709`               | `LOINC/8310-5`  | body temperature      | WeavEHR `temperature.yml`                 |
| `VITAL//710`               | `LOINC/59408-5` | SpO2 (pulse oximetry) | WeavEHR `oxygen_saturation.yml`           |
| `VITAL//2283`              | `LOINC/3150-0`  | FiO2                  | WeavEHR `fraction_of_inspired_oxygen.yml` |
| `VITAL//725`               | `LOINC/9187-6`  | urine output          | WeavEHR `urine_output.yml`                |
| `WEIGHT`                   | `LOINC/29463-7` | body weight           | `cases.WeightOnAdmission`                 |
| `HEIGHT`                   | `LOINC/8302-2`  | body height           | `cases.HeightOnAdmission`                 |

- **WeavEHR source.** The WeavEHR files are
    [aidh-ms/WeavEHR@210a539](https://github.com/aidh-ms/WeavEHR/tree/210a539/configs/datasets/sic/1.0.6/mappings),
    MIT licensed.
- **ricu.** ricu's SICdb concepts select the same DataIDs. ricu is GPL-3 and is cited for the ids
    only. ricu also files DataID 724 under heart rate. Only ricu names it, so it gets no parent here.
- **LOINC codes.** Every LOINC code above is a standard concept in LOINC 2.80. Only the codes are
    written here; suMEDS's LOINC pack adds their names. The blood-pressure
    pairs are an invasive and a non-invasive signal. The generic codes cover both without claiming
    which signal is which.
- **Checked.** The medians of every coded signal on the full 1.0.8 release (aggregates only) agree
    with its parent.

## Enriching code metadata with suMEDS

Vocabulary work lives in [suMEDS](https://github.com/rvandewater/SumMEDS) rather than in this ETL.
The package ships the rules for it,
[`src/SICdb_MEDS/sumeds_rules.yaml`](src/SICdb_MEDS/sumeds_rules.yaml), registered in suMEDS's
`sumeds.rules` entry-point group under the same name as the MEDS-Extract spec, `SICdb`, so
`--rules SICdb` works for any suMEDS command. The spec runs them as its last stage, after suMEDS's
[`sumeds_link_unit_siblings`](https://github.com/rvandewater/suMEDS/blob/main/docs/unit_siblings.md),
which links codes that differ only in their unit (SICdb has none: each `d_references` id has one
unit, so it changes nothing today, but the post-stages are the same as in the other MEDS ETLs), and
`sumeds_normalize_units`, which writes `unit` in UCUM (see **Units** above):

```yaml
etl:
  post_stages:
    - sumeds_link_unit_siblings
    - sumeds_normalize_units
    - sumeds_enrich: {rules: SICdb}
```

The stage replaces `metadata/codes.parquet` only once the enriched table is complete, and it stays
valid MEDS code metadata. The LOINC data comes from suMEDS's ontology packs, fetched
from GitHub on first use and cached; an Athena download (`athena_csv`, below) is the only source for
ICD-10-GM.

`metadata/dataset.json` says whether a build is enriched: `sumeds_enrichment.enriched` is `true`
(with the rules, pack source and coverage) or `false` (with the error and the `retry` command).

```bash
jq .sumeds_enrichment.enriched $MEDS_COHORT_DIR/metadata/dataset.json
```

Enrichment soft-fails: if it cannot run (a server without internet, for example), the ETL still
succeeds with `metadata/codes.parquet` left exactly as MEDS-Extract writes it, and `dataset.json`
records `"enriched": false`. The warning, with the command that finishes the enrichment, is in
`$MEDS_COHORT_DIR/.logs/pipeline.log`, not on the console. MEDS-Extract marks the stage done either
way, so running `meds-extract-run` again does not retry it. Enrich the build afterwards, with the
rules and options of the failed run:

```bash
suMEDS-enrich $MEDS_COHORT_DIR
# Air-gapped: from a copy of the sumeds-ontologies packs directory
suMEDS-enrich $MEDS_COHORT_DIR --ontology-source /path/to/sumeds-ontologies/ontologies --offline
```

For the plain, unenriched ETL, leave the stage out with `skip_stages=[sumeds_enrich]`:

```bash
meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR skip_stages=[sumeds_enrich]
```

The stage takes the `suMEDS-enrich` options, spelled with underscores. Set them for one run with
`overrides=`, as `++stage_cfg.<option>` (the override reaches every stage of the pipeline, and only
this one declares the option, hence the `++`):

```bash
# Make a failed enrichment fail the run (exit code 1):
meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR "overrides=['++stage_cfg.strict=true']"
# Also look codes up in your own OHDSI Athena download:
meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR "overrides=['++stage_cfg.athena_csv=/path/to/athena']"
# Read the packs from a local copy (or set SUMEDS_ONTOLOGY_SOURCE=/path/to/ontologies):
meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR "overrides=['++stage_cfg.ontology_source=/path/to/ontologies']"
```

On a cluster whose compute nodes have no internet access, fill a cache on shared storage once from
a login node, then run with `offline`, so nothing tries the network:

```bash
# The same cache on the login node and the compute nodes:
export XDG_CACHE_HOME=/shared/project/cache
# On the login node, download the pinned pack release (about 30 MB):
suMEDS-enrich --prefetch --rules SICdb
# On a compute node:
meds-extract-run spec=SICdb output_dir=$MEDS_COHORT_DIR "overrides=['++stage_cfg.offline=true']"
```

The ETL already describes every code, and suMEDS never overwrites a value, so the descriptions stay
SICdb's own. For SICdb, suMEDS adds:

- **LOINC names.** The `LOINC/…` parents of `LAB//…` codes are looked up in suMEDS's LOINC pack,
    fetched from GitHub on first use and cached; no Athena download is needed. Each gets its LOINC
    long common name in `parent_names` (a map from parent code to name), and the lab code gets
    `vocabulary_id` `LOINC` and its `concept_code`. The stage option `loinc_hierarchy`
    (`--loinc-hierarchy` for `suMEDS-enrich`) also adds the LOINC Parts above the term as parents.
- **ICD-10-GM names**, with an Athena download (`athena_csv`) only. suMEDS has no ICD-10-GM pack, so the
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
`meds-extract-run spec=SICdb` with its enrichment stage on a small synthetic release
(`tests/conftest.py`) with a local LOINC pack, and checks that the enrichment keeps every value the
plain build (`skip_stages=[sumeds_enrich]`) wrote and is recorded in `dataset.json`. It also runs
against an empty pack source, to check the soft failure and its `suMEDS-enrich` retry, and with
`strict`.

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
`meds-extract-run spec=SICdb`, so its code-metadata coverage includes the suMEDS enrichment. It covers
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

Both print how the statistics changed against the committed baseline before overwriting it, and
refuse a build whose enrichment failed (finish it with `suMEDS-enrich` first). Add
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

## Breaking changes

### Unreleased (after 0.2.0)

- **`CaseID` is renamed `icustay_id`.** The column holds the same value as before, `cases.CaseID`
    (one ICU admission), on the same events, and is still the one `raw_source_id_columns` entry in
    `metadata/dataset.json`. Event rows and codes are otherwise unchanged. `icustay_id` is the name
    the MIMIC-IV ETL and the other MEDS ETLs give the ICU stay
    ([alignEHR#55](https://github.com/rvandewater/alignEHR/issues/55)). To migrate, rename the
    column: `pl.col("CaseID")` → `pl.col("icustay_id")`, or, for output written before the change,
    `df.rename({"CaseID": "icustay_id"})`.

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
