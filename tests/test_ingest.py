import logging
from datetime import datetime, timezone

import pytest
from pydantic import ValidationError
from sqlalchemy import text

from pipeline import ingest_data
from pipeline.ingest_data import BioreactorReading, generate_bioreactor_data, insert_records


def make_reading(**overrides):
    base = dict(
        timestamp=datetime.now(timezone.utc),
        temperature_c=37.0,
        ph_level=7.1,
        biomass_g_l=5.0,
    )
    return BioreactorReading(**{**base, **overrides})


# ─── Contrato Pydantic ────────────────────────────────────
def test_valid_reading_is_accepted_and_rounded():
    r = make_reading(temperature_c=37.12345, biomass_g_l=5.123456)
    assert r.temperature_c == 37.12
    assert r.biomass_g_l == 5.123
    assert r.batch_id == "BATCH-001"


@pytest.mark.parametrize(
    "field,value",
    [
        ("temperature_c", 100.1),
        ("temperature_c", -1),
        ("ph_level", 14.5),
        ("biomass_g_l", -0.1),
        ("dissolved_o2", 101),
        ("agitation_rpm", 1501),
    ],
)

def test_out_of_hard_range_is_rejected(field, value):
    with pytest.raises(ValidationError):
        make_reading(**{field: value})


def test_missing_required_field_is_rejected():
    with pytest.raises(ValidationError):
        BioreactorReading(timestamp=datetime.now(timezone.utc), temperature_c=37.0)


def test_temperature_outside_operating_range_warns_but_is_not_blocked(caplog):
    with caplog.at_level(logging.WARNING, logger="ingest_data"):
        r = make_reading(temperature_c=52.0)
    assert r.temperature_c == 52.0
    assert "BIOREACTOR WARNING" in caplog.text


# ─── Generador sintético ──────────────────────────────────
def test_generator_returns_expected_number_of_valid_records():
    records = generate_bioreactor_data(hours=24, readings_per_hour=6)
    assert len(records) == 144
    assert {r["batch_id"] for r in records} == {"BATCH-001"}
    assert all(0 <= r["ph_level"] <= 14 for r in records)


def test_generator_injects_the_temperature_anomaly():
    records = generate_bioreactor_data(hours=24, readings_per_hour=6)
    hot = [r for r in records if r["temperature_c"] > 45]
    assert 5 <= len(hot) <= 8  # ventana de ±3 lecturas alrededor de -6h
    assert all(r["notes"] and "temperature" in r["notes"].lower() for r in hot)


def test_generator_is_reproducible_for_values():
    a = generate_bioreactor_data(hours=2, readings_per_hour=6)
    b = generate_bioreactor_data(hours=2, readings_per_hour=6)
    assert [r["temperature_c"] for r in a] == [r["temperature_c"] for r in b]


# ─── Inserción (SQLite en lugar de PostgreSQL) ────────────
def test_insert_records_replaces_previous_data(sqlite_engine, monkeypatch):
    monkeypatch.setattr(ingest_data, "create_engine", lambda *a, **k: sqlite_engine)

    insert_records(generate_bioreactor_data(hours=1, readings_per_hour=6))
    insert_records(generate_bioreactor_data(hours=2, readings_per_hour=6))

    with sqlite_engine.connect() as conn:
        count = conn.execute(text("SELECT COUNT(*) FROM bioreactor_readings")).scalar()
    assert count == 12  # la segunda carga borra la primera
