"""The ``sicdb_collapse_unit_metadata`` stage: one ``unit`` per code, as a String, in ``codes.parquet``.

MEDS-Extract writes every ``_metadata`` column as ``List(String)``, the distinct values seen for a code.
The ``unit`` of a code is meant to be a single value, the unit of its numeric values as recorded (MEDS
does not standardise this column), so this post-stage turns the list into a nullable String. It runs
first among the post-stages, so that ``sumeds_enrich`` sees (and its coverage counts) the final codes,
and a later ``suMEDS-enrich`` retry gives the same result as the build::

    etl:
      post_stages:
        - sicdb_collapse_unit_metadata
        - sumeds_enrich: {rules: SICdb}

Each list is stripped of surrounding whitespace, null and empty entries are dropped, and the distinct
values remain: none gives null, one gives that unit. A code with more than one unit is an error, not a
silent choice: the fix is to put the unit in the code for that family (as its last component). Units are
not normalised or renamed. An already-String ``unit`` passes through, and without a ``unit`` column the
file is left unchanged, so running the stage twice changes nothing.

**Unit siblings.** Then the stage links codes that differ only in their unit. A code is *unit-suffixed*
when its ``unit`` is set and the code ends with ``//<unit>``; its *stem* is the code without that
suffix. When two or more unit-suffixed codes share a stem (``LAB//<item>//mg/dL``,
``LAB//<item>//mmol/L``), the stem (``LAB//<item>``) is appended to each one's ``parent_codes``, after
any vocabulary parents and only once, and the stem gets a ``codes.parquet`` row of its own (unless one
exists): the siblings' description if they all share one, the vocabulary parents they all share, no
unit, and nulls elsewhere. A unit-suffixed code without a sibling gets no parent.

**Units stated in descriptions.** A description may state its code's unit (``DESCRIPTION_UNIT_PATTERN``,
group ``unit``). Such a unit fills ``unit`` only where the data gave none, and only if it looks like a
unit (:func:`is_unit_like`); a unit from the data always wins. When sibling descriptions differ only in
the matched span, the stem row gets the description with that span removed. SICdb's dictionary
(``d_references.ReferenceValue``) states no units in its descriptions: its trailing bracketed parts
are never the item's ``ReferenceUnit``, so ``DESCRIPTION_UNIT_PATTERN`` is ``None`` here. The codes
SICdb describes only in ``messy.yaml`` carry the unit their description names as a ``unit:`` literal.
"""

import logging
import os
import re
from pathlib import Path

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq
from meds import CodeMetadataSchema, code_metadata_filepath
from MEDS_transforms.stages import Stage

logger = logging.getLogger(__name__)

STAGE_NAME = "sicdb_collapse_unit_metadata"
UNIT_COLUMN = "unit"
MAX_EXAMPLES = 5

# The unit span of a description, as a regex with a `unit` group. None: SICdb's dictionary
# descriptions state no units (see the module docstring).
DESCRIPTION_UNIT_PATTERN: re.Pattern | None = None
# Placeholders for a missing unit, which are not units.
NOT_UNITS = frozenset({"unknown", "unk", "na", "n/a", "none", "null", "-", "?"})
UNIT_SHAPE = re.compile(r"^(?=.*[A-Za-z%°µμ])[A-Za-z0-9%°µμ/.*^·+\-\[\]{}'() ]{1,32}$")


class MultipleUnitsError(ValueError):
    """Some codes have more than one unit; ``n_codes`` says how many, ``examples`` names up to five."""

    def __init__(self, n_codes: int, examples: list[str]):
        self.n_codes = n_codes
        self.examples = examples
        super().__init__(
            f"{n_codes} codes have more than one unit in metadata/codes.parquet (e.g. {examples}). "
            "Each code must have one unit: put the unit in the code for that family, as its last "
            "component (`.../<unit>`)."
        )


def collapse_units(codes: pl.DataFrame, column: str = UNIT_COLUMN) -> pl.DataFrame:
    """Return ``codes`` with its List ``column`` reduced to one String unit per code.

    Raises:
        MultipleUnitsError: if a code has more than one distinct unit.

    Examples:
        >>> codes = pl.DataFrame(
        ...     {"code": ["A", "B", "C", "D"], "unit": [["mg"], ["mg", " mg ", ""], [None], None]}
        ... )
        >>> collapse_units(codes)["unit"].to_list()
        ['mg', 'mg', None, None]
        >>> collapse_units(collapse_units(codes)).equals(collapse_units(codes))
        True
        >>> try:
        ...     collapse_units(pl.DataFrame({"code": ["A", "B"], "unit": [["mg", "g"], ["mg"]]}))
        ... except MultipleUnitsError as error:
        ...     print(error.n_codes, error.examples)
        1 ['A']
    """
    if column not in codes.columns:
        return codes
    dtype = codes.schema[column]
    if not isinstance(dtype, pl.List):
        return codes.with_columns(pl.col(column).cast(pl.String))
    units = codes[column].list.eval(
        pl.element()
        .cast(pl.String)
        .str.strip_chars()
        .filter(pl.element().str.strip_chars() != "")
    )
    units = units.list.unique(maintain_order=True)
    several = units.list.len() > 1
    if several.any():
        examples = codes.filter(several)["code"].head(MAX_EXAMPLES).to_list()
        raise MultipleUnitsError(int(several.sum()), examples)
    return codes.with_columns(units.list.first().alias(column))


def is_unit_like(text: str) -> bool:
    """Whether ``text`` has the shape of a unit (UCUM-like characters, not a missing-unit placeholder).

    Examples:
        >>> [is_unit_like(t) for t in ["mg/dL", "10^9/L", "%", "mmHg", "UNKNOWN", "12", "see comment: x"]]
        [True, True, True, True, False, False, False]
    """
    text = text.strip()
    return text.lower() not in NOT_UNITS and bool(UNIT_SHAPE.match(text))


def units_from_descriptions(
    codes: pl.DataFrame, pattern: re.Pattern | None, column: str = UNIT_COLUMN
) -> tuple[pl.DataFrame, dict[str, int]]:
    r"""Fill a null ``column`` from the unit its description states; return the frame and counts.

    Counts: ``description_units`` (descriptions stating a unit-like unit), ``description_units_rejected``
    (matches that are not unit-like), ``description_units_filled`` (codes that gained their unit) and
    ``description_unit_conflicts`` (codes whose unit from the data differs; the data wins).

    Examples:
        >>> pattern = re.compile(r" \[(?P<unit>[^\]]+)\]$")
        >>> codes = pl.DataFrame({
        ...     "code": ["HR", "W", "X", "Y"],
        ...     "description": ["Heart rate [b/min]", "Weight [kg]", "Score [none]", "Temp [degC]"],
        ...     "unit": [None, "g", None, None],
        ... })
        >>> out, counts = units_from_descriptions(codes, pattern)
        >>> out["unit"].to_list(), counts["description_units_filled"], counts["description_unit_conflicts"]
        (['b/min', 'g', None, 'degC'], 2, 1)
    """
    counts = dict.fromkeys(
        (
            "description_units",
            "description_units_rejected",
            "description_units_filled",
            "description_unit_conflicts",
        ),
        0,
    )
    if pattern is None or "description" not in codes.columns:
        return codes, counts
    units = []
    for description, unit in zip(codes["description"], codes[column], strict=True):
        match = pattern.search(description) if description else None
        stated = match["unit"].strip() if match else None
        if stated is not None and not is_unit_like(stated):
            counts["description_units_rejected"] += 1
            stated = None
        if stated is not None:
            counts["description_units"] += 1
            if unit is None:
                counts["description_units_filled"] += 1
            elif unit != stated:
                counts["description_unit_conflicts"] += 1
        units.append(unit if unit is not None else stated)
    return codes.with_columns(pl.Series(column, units, dtype=pl.String)), counts


def without_unit_span(
    description: str | None, pattern: re.Pattern | None
) -> str | None:
    r"""``description`` with the span ``pattern`` matches removed and trimmed, or ``None`` if no match.

    Examples:
        >>> without_unit_span("Weight [kg] (chart)", re.compile(r" ?\[(?P<unit>[^\]]+)\]"))
        'Weight (chart)'
    """
    match = pattern.search(description) if pattern is not None and description else None
    if match is None:
        return None
    return (description[: match.start()] + description[match.end() :]).strip()


def unit_stem(code: str, unit: str | None) -> str | None:
    """Return ``code`` without its ``//<unit>`` suffix, or ``None`` if it does not end with its unit.

    Examples:
        >>> unit_stem("LAB//Glucose//mg/dL", "mg/dL")
        'LAB//Glucose'
        >>> unit_stem("LAB//Glucose//mg/dL", "mmol/L") is None
        True
        >>> unit_stem("VITAL//HEART_RATE", None) is None
        True
    """
    if not unit or not code.endswith(f"//{unit}"):
        return None
    return code[: -len(unit) - 2] or None


def add_unit_sibling_parents(
    codes: pl.DataFrame, column: str = UNIT_COLUMN
) -> tuple[pl.DataFrame, int, int]:
    """Append each unit sibling's stem to its ``parent_codes``.

    Returns the new frame, the number of sibling groups (stems shared by two or more unit-suffixed
    codes) and the number of codes whose ``parent_codes`` gained their stem.

    ``column`` must already be one String per code (see :func:`collapse_units`). ``parent_codes`` is
    created if missing; existing parents stay first and the stem is not added twice.

    Examples:
        >>> codes = pl.DataFrame({
        ...     "code": ["LAB//Na//mmol/L", "LAB//Na//mEq/L", "LAB//K//mmol/L", "VITAL//HR"],
        ...     "unit": ["mmol/L", "mEq/L", "mmol/L", "bpm"],
        ... })
        >>> out, groups, added = add_unit_sibling_parents(codes)
        >>> groups, added, out["parent_codes"].to_list()
        (1, 2, [['LAB//Na'], ['LAB//Na'], None, None])
        >>> add_unit_sibling_parents(out)[1:]  # idempotent
        (1, 0)
    """
    if column not in codes.columns:
        return codes, 0, 0
    stems = [
        unit_stem(code, unit)
        for code, unit in zip(codes["code"], codes[column], strict=True)
    ]
    counts: dict[str, int] = {}
    for stem in stems:
        if stem is not None:
            counts[stem] = counts.get(stem, 0) + 1
    siblings = {stem for stem, n in counts.items() if n > 1}
    current = (
        codes["parent_codes"].to_list()
        if "parent_codes" in codes.columns
        else [None] * codes.height
    )
    parents, added = [], 0
    for stem, old in zip(stems, current, strict=True):
        if stem in siblings and stem not in (old or []):
            parents.append([*(old or []), stem])
            added += 1
        else:
            parents.append(old)
    out = codes.with_columns(
        pl.Series("parent_codes", parents, dtype=pl.List(pl.String))
    )
    return out, len(siblings), added


def unit_stem_rows(
    codes: pl.DataFrame, column: str = UNIT_COLUMN, pattern: re.Pattern | None = None
) -> list[dict]:
    """Return a new ``codes.parquet`` row for each sibling-group stem that has no row yet.

    The row's ``description`` is the siblings' description if they all share one non-null value (else,
    if they differ only in the span ``pattern`` matches, the description without that span), its
    ``parent_codes`` the parents all siblings share (never the stem itself; ``None`` if none), and its
    unit is null, as are the columns the row does not name.

    Examples:
        >>> codes = pl.DataFrame({
        ...     "code": ["LAB//Na//mmol/L", "LAB//Na//mEq/L", "LAB//K//mmol/L"],
        ...     "description": ["Sodium", "Sodium", "Potassium"],
        ...     "unit": ["mmol/L", "mEq/L", "mmol/L"],
        ...     "parent_codes": [["LOINC//2951-2", "LAB//Na"], ["LOINC//2951-2", "LAB//Na"], None],
        ... })
        >>> unit_stem_rows(codes)
        [{'code': 'LAB//Na', 'description': 'Sodium', 'parent_codes': ['LOINC//2951-2']}]
    """
    if column not in codes.columns:
        return []
    existing = set(codes["code"].to_list())
    descriptions = (
        codes["description"].to_list()
        if "description" in codes.columns
        else [None] * codes.height
    )
    parents = (
        codes["parent_codes"].to_list()
        if "parent_codes" in codes.columns
        else [None] * codes.height
    )
    groups: dict[str, list[int]] = {}
    for i, (code, unit) in enumerate(zip(codes["code"], codes[column], strict=True)):
        if (stem := unit_stem(code, unit)) is not None:
            groups.setdefault(stem, []).append(i)
    rows = []
    for stem, members in groups.items():
        if len(members) < 2 or stem in existing:
            continue
        shared_descriptions = {descriptions[i] for i in members}
        if len(shared_descriptions) > 1:
            shared_descriptions = {
                without_unit_span(descriptions[i], pattern) for i in members
            }
        description = (
            shared_descriptions.pop() if len(shared_descriptions) == 1 else None
        )
        shared = [p for p in (parents[members[0]] or []) if p != stem]
        for i in members[1:]:
            shared = [p for p in shared if p in (parents[i] or [])]
        rows.append(
            {"code": stem, "description": description, "parent_codes": shared or None}
        )
    return rows


def collapse_unit_metadata(
    codes_fp: Path,
    out_fp: Path | None = None,
    pattern: re.Pattern | None = DESCRIPTION_UNIT_PATTERN,
) -> dict[str, int]:
    """Collapse ``unit`` to one String per code and link unit siblings, in a finished ``codes.parquet``.

    Args:
        codes_fp: The ``metadata/codes.parquet`` of a finished extraction.
        out_fp: Where to write the result; by default ``codes_fp`` itself.
        pattern: The unit span of a description (group ``unit``); see :func:`units_from_descriptions`.

    Returns:
        Counts: ``units`` (codes with a unit), ``sibling_groups`` (stems shared by two or more
        unit-suffixed codes), ``sibling_parents`` (codes whose ``parent_codes`` gained their stem),
        ``stem_rows`` (rows added for stems), and those of :func:`units_from_descriptions`.

    Raises:
        MultipleUnitsError: if a code has more than one unit; nothing is written then.

    Only ``unit`` (written as Arrow ``large_string``) and ``parent_codes`` are replaced and stem rows
    added; every other column keeps its Arrow type, and a file sorted by code stays sorted. The result
    is validated against the MEDS code-metadata schema and written atomically.
    """
    codes_fp = Path(codes_fp)
    out_fp = codes_fp if out_fp is None else Path(out_fp)
    table = pq.read_table(codes_fp)
    counts = {"units": 0, "sibling_groups": 0, "sibling_parents": 0, "stem_rows": 0}
    if UNIT_COLUMN in table.column_names:
        optional = [
            c for c in ("description", "parent_codes") if c in table.column_names
        ]
        columns = ["code", UNIT_COLUMN, *optional]
        before = pl.from_arrow(table.select(columns))
        codes, described = units_from_descriptions(collapse_units(before), pattern)
        counts.update(described)
        codes, counts["sibling_groups"], counts["sibling_parents"] = (
            add_unit_sibling_parents(codes)
        )
        counts["units"] = int(codes[UNIT_COLUMN].is_not_null().sum())
        # Arrow `large_string`, as suMEDS writes it back, so a retried enrichment changes no type.
        unit = codes[UNIT_COLUMN].to_arrow().cast(pa.large_string())
        table = table.set_column(
            table.column_names.index(UNIT_COLUMN), UNIT_COLUMN, unit
        )
        if counts["sibling_parents"]:
            parents = codes["parent_codes"].to_arrow().cast(pa.list_(pa.string()))
            if "parent_codes" in table.column_names:
                table = table.set_column(
                    table.column_names.index("parent_codes"), "parent_codes", parents
                )
            else:
                table = table.append_column("parent_codes", parents)
        if stem_rows := unit_stem_rows(codes, pattern=pattern):
            counts["stem_rows"] = len(stem_rows)
            was_sorted = pl.from_arrow(table.column("code")).is_sorted()
            table = pa.concat_tables(
                [table, pa.Table.from_pylist(stem_rows, schema=table.schema)]
            )
            if was_sorted:
                table = table.sort_by("code")
    CodeMetadataSchema.validate(table)

    tmp_fp = out_fp.with_name(f".{out_fp.name}.tmp")
    pq.write_table(table, tmp_fp)
    os.replace(tmp_fp, out_fp)
    return counts


@Stage.register(stage_name=STAGE_NAME, is_metadata=True)
def main(cfg) -> None:
    """Collapse ``unit`` and link unit siblings in the finished ``metadata/codes.parquet``."""
    if cfg.worker != 0:
        return
    codes = Path(code_metadata_filepath).name
    counts = collapse_unit_metadata(
        Path(cfg.stage_cfg.metadata_input_dir) / codes,
        Path(cfg.stage_cfg.reducer_output_dir) / codes,
    )
    logger.info(
        f"{counts['units']} codes have a unit (one String per code); {counts['sibling_parents']} unit "
        f"siblings in {counts['sibling_groups']} groups got their stem as a parent code; "
        f"{counts['stem_rows']} stem rows added; {counts['description_units_filled']} units filled from "
        f"descriptions ({counts['description_unit_conflicts']} disagree with the data, which wins)."
    )
