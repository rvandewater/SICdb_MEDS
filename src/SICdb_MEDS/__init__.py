from importlib.metadata import PackageNotFoundError, version
from importlib.resources import files

from omegaconf import OmegaConf

__package_name__ = "SICdb_MEDS"
try:
    __version__ = version(__package_name__)
except PackageNotFoundError:  # pragma: no cover
    __version__ = "unknown"

MAIN_CFG = files(__package_name__).joinpath("configs/main.yaml")
EVENT_CFG = files(__package_name__).joinpath("configs/event_configs.yaml")
ETL_CFG = files(__package_name__).joinpath("configs/ETL.yaml")
RUNNER_CFG = files(__package_name__).joinpath("configs/runner.yaml")
PRE_MEDS_PY = files(__package_name__).joinpath("pre_MEDS.py")
PRE_MEDS_CFG = files(__package_name__).joinpath("configs/pre_MEDS.yaml")
DATASET_CFG = files(__package_name__).joinpath("dataset.yaml")
TABLE_PROCESSOR_CFG = files(__package_name__).joinpath("configs/table_preprocessors.yaml")

dataset_info = OmegaConf.load(DATASET_CFG)

HAS_PRE_MEDS = PRE_MEDS_PY.exists()

event_config = OmegaConf.load(EVENT_CFG)

__all__ = [
    "event_config",
    "EVENT_CFG",
    "ETL_CFG",
    "HAS_PRE_MEDS",
    "PRE_MEDS_CFG",
    "MAIN_CFG",
    "RUNNER_CFG",
    "DATASET_CFG",
    "dataset_info",
    "__package_name__",
    "__version__",
]
