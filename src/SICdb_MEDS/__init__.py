from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

from omegaconf import OmegaConf

__package_name__ = "SICdb_MEDS"
try:
    __version__ = version(__package_name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

# The name this ETL registers under the `MEDS_extract.pipelines` entry-point group; it is what
# `meds-extract-run spec=...` resolves, and MEDS-Extract uses it as the default dataset name.
PIPELINE_NAME = "SICdb"

MAIN_CFG = files(__package_name__).joinpath("configs/main.yaml")
# The single MESSY file: `sources:` (raw-data fetching), `etl:` (run options), and the
# event-conversion tables. Replaces the old event_configs.yaml + ETL.yaml + dataset.yaml trio.
MESSY_CFG = files(__package_name__).joinpath("configs/messy.yaml")
TABLE_PROCESSOR_CFG = files(__package_name__).joinpath(
    "configs/table_preprocessors.yaml"
)

messy_cfg = OmegaConf.load(MESSY_CFG)

# Kept for backwards compatibility with code that read `dataset_info.dataset_name` /
# `.raw_dataset_version`; both now live in the MESSY file as their reserved 0.7 keys.
dataset_info = OmegaConf.create(
    {
        "dataset_name": messy_cfg.etl.dataset_name,
        "raw_dataset_version": messy_cfg.sources.dataset_version,
    }
)

__all__ = [
    "MAIN_CFG",
    "MESSY_CFG",
    "PIPELINE_NAME",
    "TABLE_PROCESSOR_CFG",
    "__package_name__",
    "__version__",
    "dataset_info",
    "messy_cfg",
]
