"""The SICdb MEDS ETL -- a pure-config MEDS-Extract pipeline.

This package contains no ETL code. The entire pipeline is `messy.yaml`, registered under the
`MEDS_extract.pipelines` entry-point group, so it runs as:

    meds-extract-run spec=SICdb output_dir=$OUTPUT_DIR
"""

from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

__package_name__ = "SICdb_MEDS"
try:
    __version__ = version(__package_name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

# The name this ETL registers under `MEDS_extract.pipelines`; `meds-extract-run spec=SICdb`
# resolves it, and MEDS-Extract uses it as the default dataset name.
PIPELINE_NAME = "SICdb"

MESSY_CFG = files(__package_name__).joinpath("messy.yaml")

__all__ = ["MESSY_CFG", "PIPELINE_NAME", "__package_name__", "__version__"]
