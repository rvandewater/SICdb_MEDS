"""Code metadata (``description``) for ``metadata/codes.parquet``.

Two routes, matching how MEDS-Transforms 0.2.x attaches metadata (see the README section "Code
metadata"):

* **Component-coded events** (diagnoses, labs, vitals, medications): pre-MEDS writes one small table
  per family next to the pre-MEDS tables (:func:`write_metadata_tables`), and the ``_metadata`` blocks
  in ``configs/event_configs.yaml`` join them onto the codes in the ``extract_code_metadata`` stage. The
  tables are built from SICdb's own dictionary at ETL time (``cases.ICD10MainText`` and
  ``d_references``), so nothing from the dataset is shipped with this package.
* **Literal codes** (``MEDS_BIRTH``, ``ADMISSION``, ``WEIGHT``, ...): a literal code has no
  component to join on, so ``metadata/static_code_descriptions.csv`` (with the source of every row) is
  filled in after the pipeline by :func:`finalize_code_metadata`, which also writes every code of the
  data into ``codes.parquet`` with the MEDS code-metadata schema.

Vocabulary parents (``ICD10//...``, ``LOINC//...``) are not added here: suMEDS adds them from the
packaged rules file ``sumeds_rules.yaml`` (README section "Enriching code metadata with suMEDS").
"""

import os
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq
from loguru import logger
from meds import code_metadata_schema

METADATA_DIR = Path(__file__).parent / "metadata"
STATIC_DESCRIPTIONS_CSV = METADATA_DIR / "static_code_descriptions.csv"

# File prefixes in the pre-MEDS directory; must match the `_metadata` blocks in `configs/event_configs.yaml`.
DIAGNOSIS_METADATA = "diagnosis_metadata"
LABORATORY_METADATA = "laboratory_metadata"
REFERENCE_METADATA = "reference_metadata"

DESCRIPTION_SEPARATOR = (
    "\n"  # The `description_separator` MEDS-Transforms uses for duplicate descriptions.
)


def _with_unit(name: pl.Expr, unit: pl.Expr, what: pl.Expr) -> pl.Expr:
    """``"{name}, {unit} ({what})"``, or ``"{name} ({what})"`` when the unit is missing."""
    return (
        pl.when(unit.is_null())
        .then(pl.format("{} ({})", name, what))
        .otherwise(pl.format("{}, {} ({})", name, unit, what))
    )


def diagnosis_metadata(cases: pl.LazyFrame) -> pl.LazyFrame:
    """One row per ``ICD10Main``: SICdb's own ``ICD10MainText``.

    A code recorded with more than one text gets all of them, sorted and joined as MEDS-Transforms joins
    duplicate descriptions. ICD-10 parents are added by suMEDS, not here.

    Examples:
        >>> cases = pl.LazyFrame({
        ...     "ICD10Main": ["I10", "S62.60", "E11.9", "E11.9", "X1.x", None],
        ...     "ICD10MainText": ["hypertension", "finger fracture", "diabetes b", "diabetes a", "x", "y"],
        ... })
        >>> diagnosis_metadata(cases).collect().rows()  # doctest: +NORMALIZE_WHITESPACE
        [('E11.9', 'diabetes a\\ndiabetes b'), ('I10', 'hypertension'), ('S62.60', 'finger fracture'),
         ('X1.x', 'x')]
    """
    return (
        cases.select(
            pl.col("ICD10Main").cast(pl.String), pl.col("ICD10MainText").cast(pl.String)
        )
        .filter(pl.col("ICD10Main").is_not_null())
        .group_by("ICD10Main")
        .agg(description=pl.col("ICD10MainText").drop_nulls().unique().sort())
        .with_columns(
            description=pl.when(pl.col("description").list.len() > 0).then(
                pl.col("description").list.join(DESCRIPTION_SEPARATOR)
            ),
        )
        .sort("ICD10Main")
    )


def laboratory_metadata(laboratory: pl.LazyFrame) -> pl.LazyFrame:
    """One row per (``LOINC_code``, ``ReferenceUnit``), the components of a ``LAB//LOINC//...`` code.

    Built from the pre-MEDS laboratory table, so only analytes that occur in it contribute. The
    description is the analyte's ``d_references.ReferenceValue`` and unit; analytes that share a code
    (same LOINC code and unit, or no LOINC code) are all listed. The ``LOINC_code`` is SICdb's own and
    stays in the code; suMEDS validates it against LOINC and adds the ``LOINC//`` parent.

    Examples:
        >>> lab = pl.LazyFrame({
        ...     "LOINC_code": ["2160-0", "2160-0", None, None, "LOCAL-1"],
        ...     "ReferenceUnit": ["mg/dl", "mg/dl", "U/l", "U/l", None],
        ...     "ReferenceValue": ["Creatinine", "Creatinine", "Enzyme B", "Enzyme A", "Local test"],
        ... })
        >>> laboratory_metadata(lab).collect().rows()  # doctest: +NORMALIZE_WHITESPACE
        [('2160-0', 'mg/dl', 'Creatinine, mg/dl (laboratory result)'),
         ('LOCAL-1', None, 'Local test (laboratory result)'),
         (None, 'U/l', 'Enzyme A, U/l (laboratory result)\\nEnzyme B, U/l (laboratory result)')]
    """
    return (
        laboratory.select(
            pl.col("LOINC_code").cast(pl.String),
            pl.col("ReferenceUnit").cast(pl.String),
            pl.col("ReferenceValue").cast(pl.String),
        )
        .unique()
        .filter(pl.col("ReferenceValue").is_not_null())
        .with_columns(
            description=_with_unit(
                pl.col("ReferenceValue"),
                pl.col("ReferenceUnit"),
                pl.lit("laboratory result"),
            )
        )
        .group_by("LOINC_code", "ReferenceUnit")
        .agg(pl.col("description").sort().str.join(DESCRIPTION_SEPARATOR))
        .sort("LOINC_code", "ReferenceUnit", nulls_last=True)
    )


def reference_metadata(references: pl.LazyFrame) -> pl.LazyFrame:
    """Descriptions of the codes built from ``d_references`` (sex, vitals, medications).

    ``SEX//{cases.Sex}`` carries a raw reference id, so its description is that reference's
    ``ReferenceValue`` (the ``Sex`` column here is ``ReferenceGlobalID``, named for the join). The other
    templates use only the columns that form the code, so a ``d_references`` row of another kind that
    happens to produce the same code produces the same text.

    Examples:
        >>> refs = pl.LazyFrame({
        ...     "ReferenceGlobalID": [1, 2],
        ...     "ReferenceName": ["Group A", "Group B"],
        ...     "ReferenceValue": ["Heart rate", "Drug X"],
        ...     "ReferenceUnit": ["bpm", None],
        ... })
        >>> for row in reference_metadata(refs).collect().iter_rows(named=True):
        ...     print(row["Sex"], row["sex_description"])
        ...     print(row["vital_description"])
        ...     print(row["medication_end_description"])
        ...     print(row["medication_description"])
        1 Sex: Heart rate (cases.Sex)
        Heart rate, bpm (hourly aggregated signal)
        Heart rate, bpm (end of drug administration, total amount given; ReferenceName Group A)
        Start of a drug administration, any drug with ReferenceName Group A (the drug is not part of this code)
        2 Sex: Drug X (cases.Sex)
        Drug X (hourly aggregated signal)
        Drug X (end of drug administration, total amount given; ReferenceName Group B)
        Start of a drug administration, any drug with ReferenceName Group B (the drug is not part of this code)
    """
    name, value, unit = (
        pl.col(c).cast(pl.String)
        for c in ("ReferenceName", "ReferenceValue", "ReferenceUnit")
    )
    end_suffix = pl.format(
        "end of drug administration, total amount given; ReferenceName {}", name
    )
    return references.select(
        pl.col("ReferenceGlobalID").alias("Sex"),
        name.alias("ReferenceName"),
        value.alias("ReferenceValue"),
        unit.alias("ReferenceUnit"),
        sex_description=pl.format("Sex: {} (cases.Sex)", value),
        vital_description=_with_unit(value, unit, pl.lit("hourly aggregated signal")),
        medication_end_description=_with_unit(value, unit, end_suffix),
        medication_description=pl.format(
            "Start of a drug administration, any drug with ReferenceName {} (the drug is not part of this code)",
            name,
        ),
    ).unique(maintain_order=True)


def write_metadata_tables(pre_MEDS_dir: Path, references: pl.LazyFrame) -> None:
    """Write the tables the ``_metadata`` blocks read, next to the pre-MEDS tables.

    Reads the pre-MEDS ``cases`` and ``laboratory`` tables (which must already be written) and the
    ``d_references`` table. Each table is written even if its source is missing, so that
    ``extract_code_metadata`` always finds it.
    """
    pre_MEDS_dir = Path(pre_MEDS_dir)
    cases_fp = pre_MEDS_dir / "cases.parquet"
    lab_fp = pre_MEDS_dir / "laboratory.parquet"

    if cases_fp.is_file():
        diagnoses = diagnosis_metadata(pl.scan_parquet(cases_fp))
    else:
        logger.warning(f"No {cases_fp}; writing an empty {DIAGNOSIS_METADATA} table.")
        diagnoses = diagnosis_metadata(
            pl.LazyFrame(schema={"ICD10Main": pl.String, "ICD10MainText": pl.String})
        )

    if lab_fp.is_file():
        lab = pl.scan_parquet(lab_fp)
        if "ReferenceValue" not in lab.collect_schema().names():
            raise ValueError(
                f"{lab_fp} has no ReferenceValue column; it was written by an older version of this ETL. "
                "Delete it and re-run so pre-MEDS rebuilds it."
            )
    else:
        logger.warning(f"No {lab_fp}; writing an empty {LABORATORY_METADATA} table.")
        lab = pl.LazyFrame(
            schema={
                c: pl.String for c in ("LOINC_code", "ReferenceUnit", "ReferenceValue")
            }
        )

    for prefix, table in [
        (DIAGNOSIS_METADATA, diagnoses),
        (LABORATORY_METADATA, laboratory_metadata(lab)),
        (REFERENCE_METADATA, reference_metadata(references)),
    ]:
        out_fp = pre_MEDS_dir / f"{prefix}.parquet"
        df = table.collect()
        df.write_parquet(out_fp)
        logger.info(f"Wrote {df.height} rows of code metadata to {out_fp}")


def read_static_code_descriptions(path: Path = STATIC_DESCRIPTIONS_CSV) -> pl.DataFrame:
    """Read the packaged descriptions of literal codes; the ``source`` column documents the table only.

    Examples:
        >>> df = read_static_code_descriptions()
        >>> df.columns
        ['code', 'description']
        >>> df["code"].is_duplicated().any()
        False
    """
    return pl.read_csv(path, infer_schema=False).select("code", "description")


def finalize_code_metadata(
    MEDS_cohort_dir: Path, static: pl.DataFrame | None = None
) -> dict[str, int]:
    """Rewrite ``metadata/codes.parquet`` to list every code of the data with its metadata.

    The ``description`` and ``parent_codes`` extracted by the pipeline are kept; a missing description
    is filled from the static table (an extracted description always wins); parent lists are deduplicated
    and an empty list becomes null. The file is written with the MEDS code-metadata schema (``string`` and
    ``list<string>``, not polars' ``large_string``) and checked against it before it replaces the old one.

    Args:
        MEDS_cohort_dir: The finished MEDS cohort directory (with ``data/`` and ``metadata/``).
        static: Override of the packaged static table (``code``, ``description``), for tests.

    Returns:
        Counts of codes, codes with a description, and codes with parent codes.
    """
    MEDS_cohort_dir = Path(MEDS_cohort_dir)
    codes_fp = MEDS_cohort_dir / "metadata" / "codes.parquet"
    if static is None:
        static = read_static_code_descriptions()

    schema = code_metadata_schema()
    if codes_fp.is_file():
        present = set(pq.read_schema(codes_fp).names)
        extracted = pl.from_arrow(
            pq.read_table(codes_fp, columns=[c for c in schema.names if c in present])
        )
        for name in set(schema.names) - present:
            dtype = pl.List(pl.String) if name == "parent_codes" else pl.String
            extracted = extracted.with_columns(pl.lit(None, dtype).alias(name))
    else:
        extracted = pl.from_arrow(schema.empty_table())
    extracted = extracted.unique("code", keep="first")

    codes = (
        pl.scan_parquet(MEDS_cohort_dir / "data" / "**" / "*.parquet")
        .select(pl.col("code").cast(pl.String))
        .unique()
        .collect()
        .join(
            extracted.with_columns(pl.col("code").cast(pl.String)),
            on="code",
            how="left",
        )
        .join(static.rename({"description": "_static"}), on="code", how="left")
        .with_columns(
            description=pl.coalesce(pl.col("description").cast(pl.String), "_static"),
            parent_codes=pl.col("parent_codes")
            .cast(pl.List(pl.String))
            .list.drop_nulls()
            .list.unique(maintain_order=True),
        )
        .with_columns(
            parent_codes=pl.when(pl.col("parent_codes").list.len() > 0).then(
                "parent_codes"
            )
        )
        .select(schema.names)
        .sort("code")
    )

    table = codes.to_arrow().cast(schema)
    if not table.schema.equals(schema):  # pragma: no cover - cast() raises first
        raise ValueError(
            f"codes.parquet does not match the MEDS code-metadata schema: {table.schema}"
        )

    codes_fp.parent.mkdir(parents=True, exist_ok=True)
    tmp_fp = codes_fp.with_suffix(".parquet.tmp")
    pq.write_table(table, tmp_fp)
    os.replace(tmp_fp, codes_fp)

    counts = {
        "codes": codes.height,
        "described": int(codes["description"].is_not_null().sum()),
        "with_parents": int(codes["parent_codes"].is_not_null().sum()),
    }
    logger.info(f"Wrote {codes_fp}: {counts}")
    return counts
