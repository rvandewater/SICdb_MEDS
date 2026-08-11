#!/usr/bin/env python
"""End-to-end driver for the SICdb MEDS ETL.

Under MEDS-Extract 0.7 the download leg and the extraction pipeline are both provided by
MEDS-Extract itself (`meds-extract-download` / `meds-extract-run`), driven by the bundled
`configs/messy.yaml`. All this module still does is sequence them around the pre-MEDS step,
which is the one part of the ETL that is not yet expressible in MESSY (unzipping the PhysioNet
archive, resolving SICdb's relative offsets into absolute timestamps, and optionally unpacking
the high-resolution waveform tables).

If you do not need the pre-MEDS step re-run, skip this wrapper entirely:

    meds-extract-run spec=SICdb output_dir=$OUTPUT_DIR
"""

import logging
import subprocess
import sys
from pathlib import Path

import hydra
from omegaconf import DictConfig

from . import MAIN_CFG, MESSY_CFG, PIPELINE_NAME
from .pre_MEDS import main as pre_MEDS_transform

logger = logging.getLogger(__name__)


def run_command(command_parts: list[str]) -> None:
    """Run a subprocess, streaming its output, and raise if it fails.

    Args:
        command_parts: The argv list to run.

    Raises:
        RuntimeError: If the command exits non-zero.
    """
    logger.info("Running command: %s", " ".join(command_parts))
    result = subprocess.run(command_parts, check=False)
    if result.returncode != 0:
        raise RuntimeError(
            f"Command {' '.join(command_parts)} failed with return code {result.returncode}."
        )


@hydra.main(version_base=None, config_path=str(MAIN_CFG.parent), config_name=MAIN_CFG.stem)
def main(cfg: DictConfig):
    """Runs the end-to-end MEDS extraction pipeline."""
    raw_input_dir = Path(cfg.raw_input_dir)
    pre_MEDS_dir = Path(cfg.pre_MEDS_dir)
    MEDS_cohort_dir = Path(cfg.MEDS_cohort_dir)

    # Step 0: Data downloading -- `sources:` in messy.yaml, staged by meds-extract-download.
    if cfg.do_download:  # pragma: no cover
        logger.info("Downloading raw data.")
        run_command(
            [
                "meds-extract-download",
                f"spec={MESSY_CFG!s}",
                f"output_dir={raw_input_dir.resolve()!s}",
                "key=dataset",
                f"do_overwrite={cfg.get('do_overwrite', False)}",
            ]
        )
    else:  # pragma: no cover
        logger.info("Skipping data download.")

    # Step 1: Pre-MEDS data wrangling.
    pre_MEDS_transform(
        cfg,
        input_dir=raw_input_dir,
        output_dir=pre_MEDS_dir,
        do_overwrite=cfg.get("do_overwrite", None),
    )

    # Step 2: The canonical 8-stage MEDS-Extract pipeline. The raw data is already staged and
    # pre-MEDS-processed, so downloading is disabled and `input_dir` points at the pre-MEDS output.
    run_command(
        [
            "meds-extract-run",
            f"spec={PIPELINE_NAME}",
            f"output_dir={MEDS_cohort_dir.resolve()!s}",
            "download_key=null",
            f"input_dir={pre_MEDS_dir.resolve()!s}",
        ]
    )


if __name__ == "__main__":
    sys.exit(main())
