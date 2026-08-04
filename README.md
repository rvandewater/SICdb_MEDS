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

## Configuration

The entire ETL is described by one file, `src/SICdb_MEDS/configs/messy.yaml` — a
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

The `MEDS_extract-SICdb` wrapper still exists because SICdb needs a pre-MEDS step that MESSY
cannot yet express: unzipping the PhysioNet archive, resolving relative offsets into absolute
timestamps, and optionally unpacking the high-resolution waveform tables.

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
