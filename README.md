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

The SICdb dataset offers insights into over 27 thousand intensive care admissions, including therapies and data on
preceding surgeries. Data were collected between 2013 and 2021 from four different intensive care units at the
University Hospital Salzburg, having more than 3 thousand intensive care admissions per year on 41 beds. The dataset is
deidentified and contains, amongst others, case information, vital signs, laboratory results and medication data. SICdb
provides both aggregated once-per-hour and highly granular once-per-minute data, making it suitable for computational
and machine learning-based research. (source: https://www.sicdb.com/Documentation/Main_Page)

## Usage

```bash
pip install PACKAGE_NAME # you can do this locally or via PyPI
# Download your data or set download credentials
COMMAND_NAME root_output_dir=$ROOT_OUTPUT_DIR
```

See the [MIMIC-IV MEDS Extraction ETL](https://github.com/mmcdermott/MIMIC_IV_MEDS) for an end to end example!

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