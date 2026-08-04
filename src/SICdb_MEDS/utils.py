"""Small filesystem helpers the pre-MEDS step needs.

These used to be imported from ``MEDS_transforms.utils``, but that module was pared back over the
0.3-0.6 series and ``get_shard_prefix`` / ``write_lazyframe`` no longer exist there (as of
MEDS-Transforms 0.6.7 the module exports only ``PKG_PFX``, ``Path``, ``files``, ``os`` and
``resolve_pkg_path``). They are small and purely local, so they live here now rather than
tracking an upstream module that no longer wants them.
"""

from pathlib import Path

import polars as pl


def get_shard_prefix(base_path: Path, fp: Path) -> str:
    """Extract the table prefix from a file path by removing ``base_path`` and all suffixes.

    Args:
        base_path: The base path to strip.
        fp: The file path to extract the prefix from.

    Returns:
        The file path relative to ``base_path``, with every suffix removed.

    Examples:
        >>> get_shard_prefix(Path("/a/b/c"), Path("/a/b/c/d.parquet"))
        'd'
        >>> get_shard_prefix(Path("/a/b/c"), Path("/a/b/c/d/e.csv.gz"))
        'd/e'
    """
    relative_path = fp.relative_to(base_path)
    relative_parent = relative_path.parent
    file_name = relative_path.name.split(".")[0]

    return str(relative_parent / file_name)


def write_lazyframe(df: pl.LazyFrame, out_fp: Path) -> None:
    """Collect ``df`` if needed and write it to ``out_fp`` as parquet, creating parent dirs.

    Args:
        df: The frame to write. Eager frames are accepted and written as-is.
        out_fp: The destination parquet path.

    Examples:
        >>> import tempfile
        >>> tmp = Path(tempfile.mkdtemp())
        >>> write_lazyframe(pl.LazyFrame({"a": [1, 2]}), tmp / "nested" / "out.parquet")
        >>> pl.read_parquet(tmp / "nested" / "out.parquet")["a"].to_list()
        [1, 2]
    """
    if isinstance(df, pl.LazyFrame):
        df = df.collect()
    out_fp.parent.mkdir(parents=True, exist_ok=True)
    df.write_parquet(out_fp, use_pyarrow=True)
