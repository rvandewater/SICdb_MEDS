# Development

This package is a pure-config [MEDS-Extract](https://github.com/mmcdermott/MEDS_extract) pipeline.
The whole ETL is `src/SICdb_MEDS/messy.yaml`, registered under the `MEDS_extract.pipelines` entry
point as `SICdb`. There is no ETL code to change, only that file.

## Setup

```bash
uv sync
uv run pre-commit install
```

`uv sync` installs the `dev` dependency group (pre-commit, pytest) by default, so no `--extra`
flags are needed.

## Tests

```bash
uv run pytest src/ tests/ -v
```

The tests parse the bundled config against the installed MEDS-Extract and run it on a small
synthetic release. They need no raw data, credentials or network. CI runs them on Python 3.11 and
3.12.

- `tests/test_code_metadata.py` checks that every event block describes its codes.
- `tests/test_sumeds_rules.py` loads `sumeds_rules.yaml` by its registered name, `SICdb`, and runs
    the rules, offline, on codes that MEDS-Extract renders from the config.
- `tests/test_run_with_enrichment.py` runs `suMEDS-extract spec=SICdb` on the made-up release of
    `tests/conftest.py` with a local LOINC pack, and checks that the enrichment keeps every value
    the ETL wrote.

`test_built_codes_metadata` checks a finished build instead. It validates `codes.parquet` against
the MEDS schema and checks that every code in the data has a description. It reports counts per code
family only:

```bash
SICDB_MEDS_OUTPUT_DIR=$OUT uv run pytest tests/test_code_metadata.py
```

## Running the ETL locally

SICdb is credentialed PhysioNet data. Never commit raw files or MEDS outputs. In issues and PRs,
report aggregate numbers only.

```bash
uv run suMEDS-extract spec=SICdb output_dir=$OUT do_download=false input_dir=/path/to/SICdb/1.0.8
```

This runs `meds-extract-run` with the same arguments and then enriches `metadata/codes.parquet` with
the suMEDS rules (see the README). Run `meds-extract-run` instead for the plain ETL.

A full run over release 1.0.8 takes a couple of minutes on a laptop and produces 68,584,759 rows
over 21,566 subjects.

The top-level CLI log does not show stage warnings. Each stage writes its own log under
`$OUT/<stage>/.logs/`, and `$OUT/.logs/pipeline.log` collects them. A healthy run has two kinds:

- One `extract_code_metadata` warning, documented in `messy.yaml`.
- Drop-accounting warnings from `convert_to_MEDS_events`. They count rows that produce no event by
    design, such as a weight of `0` (the missing-value placeholder) or a case with no heart surgery.

## Versioning and releases

The version is static in `pyproject.toml` and managed with
[`uv version`](https://docs.astral.sh/uv/reference/cli/#uv-version). A release is a git tag that
is exactly that version, with no `v` prefix (`0.1.0`, not `v0.1.0`).

### Choosing the part to bump

Until 1.0:

- **minor** (`0.1.0` → `0.2.0`): anything that changes existing output, such as codes, values,
    times or which events are emitted. Also a change to how the ETL is run, or a move to a new minor
    version of MEDS-Extract.
- **patch** (`0.1.0` → `0.1.1`): fixes and additions that leave existing codes and values as they
    were.

### Cutting a release

From an up-to-date `main` with a clean working tree:

```bash
git switch main && git pull
uv version --bump minor                 # or patch / major; rewrites pyproject.toml and uv.lock
VERSION=$(uv version --short)
uv build                                # optional: check dist/ shows $VERSION
git add pyproject.toml uv.lock
git commit -m "Release $VERSION"
git tag -a "$VERSION" -m "Release $VERSION"
git push origin main "$VERSION"
```

`uv version --bump minor --dry-run` shows the new version without changing anything.

Pushing the tag runs `.github/workflows/python-build.yaml`, which does the rest:

1. **Builds** the sdist and wheel. Before that, it checks that the tag equals the
    `pyproject.toml` version, and fails otherwise.
2. **Publishes to PyPI** through trusted publishing (the `pypi` environment).
3. **Signs** the distributions with Sigstore.
4. **Creates the GitHub release** with the signed files attached.

> [!IMPORTANT]
> Do not create the GitHub release yourself first. The workflow's `gh release create` step fails
> if the release already exists. Push only the tag, then edit the release notes on GitHub
> afterwards, since the workflow creates the release with empty notes.

If the tag check fails, nothing has been published. Delete the tag, fix the version, and tag again:

```bash
git tag -d "$VERSION" && git push origin ":refs/tags/$VERSION"
```

Between releases, `pyproject.toml` holds the last released version, so a development install
reports that version. Before the switch to static versions, setuptools-scm reported it as a
`.devN` version instead.

## CI

| Workflow                 | Runs on                                             | Does                                 |
| ------------------------ | --------------------------------------------------- | ------------------------------------ |
| `tests.yaml`             | pushes to `main`; PRs to `main`, `dev`, `release/*` | pytest on Python 3.11 and 3.12       |
| `code-quality-pr.yaml`   | PRs to `main`, `dev`, `release/*`                   | pre-commit on the changed files      |
| `code-quality-main.yaml` | pushes to `main`                                    | pre-commit on all files              |
| `python-build.yaml`      | every push                                          | builds; publishes only on tag pushes |

Workflows on a PR from a first-time contributor's fork wait for a maintainer to click **Approve
and run workflows** on the PR page.

The MESSY_synth synthetic-data workflow was removed when the config moved to MEDS-Extract 0.8,
because MESSY_synth still pins `MEDS-extract<0.8`. Once a MESSY_synth release supports 0.8, restore
it with:

```bash
git checkout fcc6c21 -- .github/workflows/synthetic-etl.yaml
```
