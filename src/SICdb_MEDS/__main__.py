#!/usr/bin/env python

import logging
import os
from pathlib import Path

import hydra
from omegaconf import DictConfig

from . import HAS_PRE_MEDS, MAIN_CFG
from . import __version__ as PKG_VERSION
from . import dataset_info
from .commands import run_command
from .download import download_data

if HAS_PRE_MEDS:
    from .pre_MEDS import main as pre_MEDS_transform

logger = logging.getLogger(__name__)


@hydra.main(version_base=None, config_path=str(MAIN_CFG.parent), config_name=MAIN_CFG.stem)
def main(cfg: DictConfig):
    """Runs the end-to-end MEDS Extraction pipeline."""

    raw_input_dir = Path(cfg.raw_input_dir)
    pre_MEDS_dir = Path(cfg.pre_MEDS_dir)
    MEDS_cohort_dir = Path(cfg.MEDS_cohort_dir)
    stage_runner_fp = cfg.get("stage_runner_fp", None)
    n_workers = os.environ.get("N_WORKERS")
    if n_workers is None:
        logger.info("Running in serial mode as N_WORKERS is not set.")
        os.environ["N_WORKERS"] = "1"
    else:
        logger.info(f"Running with N_WORKERS={n_workers}")


    # Step 0: Data downloading
    if cfg.do_download:  # pragma: no cover
        if cfg.get("do_demo", False):
            logger.info("Downloading demo data.")
            download_data(raw_input_dir, dataset_info, do_demo=True)
        else:
            logger.info("Downloading data.")
            download_data(raw_input_dir, dataset_info)
    else:  # pragma: no cover
        logger.info("Skipping data download.")
    if cfg.do_process_waveform:
        logger.warning(
            "Processing waveform data is enabled; "
            "this takes an extra hour and can take up to 100GB in RAM with current MEDS_transforms."
        )
    # Step 1: Pre-MEDS Data Wrangling
    if HAS_PRE_MEDS:
        pre_MEDS_transform(cfg)
    else:
        pre_MEDS_dir = raw_input_dir

    # Step 2: MEDS Cohort Creation
    # First we need to set some environment variables
    command_parts = [
        f"DATASET_NAME={dataset_info.dataset_name}",
        f"DATASET_VERSION={dataset_info.raw_dataset_version}:{PKG_VERSION}",
        f"PRE_MEDS_DIR={str(pre_MEDS_dir.resolve())}",
        f"MEDS_COHORT_DIR={str(MEDS_cohort_dir.resolve())}",
    ]


    # Then we construct the rest of the command
    command_parts.extend(
        [
            "MEDS_transform-pipeline",
            "pkg://SICdb_MEDS.configs.ETL.yaml",
        ]
    )
    if int(os.getenv("N_WORKERS", 1)) <= 1:
        logger.info("Running in serial mode as N_WORKERS is not set.")

    if stage_runner_fp:
        command_parts.append(f"stage_runner_fp={stage_runner_fp}")

    command_parts.extend(
        [
            "--overrides",
            "event_conversion_config_fp=pkg://ETL_MEDS.configs.event_configs.yaml",
        ]
    )
    run_command(command_parts, cfg)


if __name__ == "__main__":
    main()
