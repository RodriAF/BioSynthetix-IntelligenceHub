import numpy as np
import pandas as pd
import pytest
from sqlalchemy import text

from pipeline import detect_anomalies as da
from pipeline.ingest_data import generate_bioreactor_data


@pytest.fixture
def readings():
    """Datos del generador real (144 lecturas, 7 con pico de temperatura ~52 °C)."""
    df = pd.DataFrame(generate_bioreactor_data(hours=24, readings_per_hour=6))
    df["id"] = range(1, len(df) + 1)
    return df


def test_flags_most_of_the_temperature_spike(readings):
    out = da.detect_anomalies(readings)
    hot = out["temperature_c"] > 45
    flagged = out["is_anomaly"]
    assert hot.sum() == 7
    assert (flagged & hot).sum() >= 4          # recall razonable sobre el pico
    assert (flagged & hot).sum() / flagged.sum() >= 0.7  # y casi todo lo marcado es el pico


def test_anomaly_rate_is_close_to_contamination(readings):
    out = da.detect_anomalies(readings)
    assert out["is_anomaly"].mean() == pytest.approx(da.CONTAMINATION, abs=0.02)


def test_spike_points_have_lower_scores_than_normal_points(readings):
    out = da.detect_anomalies(readings)
    hot = out["temperature_c"] > 45
    assert out.loc[hot, "anomaly_score"].median() < out.loc[~hot, "anomaly_score"].median()


def test_detect_does_not_mutate_input_and_adds_columns(readings):
    before = readings.copy()
    out = da.detect_anomalies(readings)
    pd.testing.assert_frame_equal(readings, before)
    assert {"is_anomaly", "anomaly_score"} <= set(out.columns)


def test_detection_is_deterministic(readings):
    a = da.detect_anomalies(readings)
    b = da.detect_anomalies(readings)
    assert a["is_anomaly"].tolist() == b["is_anomaly"].tolist()


def test_load_and_update_roundtrip(sqlite_engine, readings):
    with sqlite_engine.begin() as conn:
        for i, r in enumerate(readings.itertuples(), start=1):
            conn.execute(
                text(
                    "INSERT INTO bioreactor_readings (timestamp, temperature_c, ph_level, biomass_g_l, dissolved_o2) "
                    "VALUES (:ts, :t, :p, :b, :o)"
                ),
                dict(ts=f"2026-01-01 {i // 60:02d}:{i % 60:02d}:00", t=r.temperature_c,
                     p=r.ph_level, b=r.biomass_g_l, o=r.dissolved_o2),
            )

    df = da.load_data(sqlite_engine)
    assert len(df) == len(readings)

    result = da.detect_anomalies(df)
    da.update_anomaly_flags(result, sqlite_engine)

    with sqlite_engine.connect() as conn:
        stored = conn.execute(text("SELECT id FROM bioreactor_readings WHERE is_anomaly = 1")).scalars().all()
    assert set(stored) == set(result.loc[result["is_anomaly"], "id"])
    assert len(stored) > 0

