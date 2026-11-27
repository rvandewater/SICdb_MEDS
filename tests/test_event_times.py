"""Event times over the synthetic release (see `conftest.py`)."""

import polars as pl


def test_survival_observation_window_is_not_before_the_death(events: pl.DataFrame):
    """`SURVIVAL_OBSERVATION_WINDOW//…` exists only on cases with a death within the window, so it is
    placed at that death, never at the admission where it would reveal the outcome (#27)."""
    window = events.filter(
        pl.col("code").str.starts_with("SURVIVAL_OBSERVATION_WINDOW//")
    )
    death = events.filter(pl.col("code") == "MEDS_DEATH").select(
        "subject_id", pl.col("time").alias("death_time")
    )
    assert window.height > 0, "the synthetic release has a case with a survival window"
    assert window["time"].null_count() == 0
    joined = window.join(death, on="subject_id", how="left")
    assert joined["death_time"].null_count() == 0
    assert (joined["time"] == joined["death_time"]).all()
