"""Tests del motor Text-to-SQL. El LLM se sustituye por una respuesta fija: no hace falta Ollama."""
import pytest
from langchain_core.language_models.fake import FakeListLLM
from sqlalchemy import text

from chat import llm_chat
from chat.llm_chat import BioReactorChatEngine


@pytest.fixture
def engine(sqlite_engine, monkeypatch):
    monkeypatch.setattr(llm_chat, "create_engine", lambda *a, **k: sqlite_engine)
    with sqlite_engine.begin() as conn:
        conn.execute(text(
            "INSERT INTO bioreactor_readings (timestamp, temperature_c, ph_level, biomass_g_l, is_anomaly) "
            "VALUES ('2026-01-01 00:00:00', 37.0, 7.1, 5.0, 0), ('2026-01-01 00:10:00', 52.0, 7.1, 5.0, 1)"
        ))
    return BioReactorChatEngine()


# ─── _extract_sql ─────────────────────────────────────────
@pytest.mark.parametrize(
    "raw,expected",
    [
        ("SELECT 1;", "SELECT 1;"),
        ("```sql\nSELECT * FROM bioreactor_readings;\n```", "SELECT * FROM bioreactor_readings;"),
        ("Here you go: SELECT 1; hope it helps", "SELECT 1;"),
        ("select 1; select 2;", "select 1;"),
    ],
)
def test_extract_sql(engine, raw, expected):
    assert engine._extract_sql(raw) == expected


# ─── Guardarraíl de solo lectura ──────────────────────────
@pytest.mark.parametrize(
    "sql",
    [
        "DELETE FROM bioreactor_readings",
        "DROP TABLE bioreactor_readings",
        "UPDATE bioreactor_readings SET is_anomaly = 1",
        "INSERT INTO bioreactor_readings (timestamp) VALUES (NOW())",
    ],
)

def test_execute_sql_blocks_writes(engine, sql):
    with pytest.raises(ValueError, match="Security"):
        engine._execute_sql(sql)


def test_execute_sql_runs_select_and_with(engine):
    rows, n = engine._execute_sql("SELECT temperature_c FROM bioreactor_readings WHERE is_anomaly = 1")
    assert n == 1 and rows[0]["temperature_c"] == 52.0

    rows, n = engine._execute_sql("WITH t AS (SELECT * FROM bioreactor_readings) SELECT COUNT(*) AS c FROM t")
    assert rows[0]["c"] == 2


@pytest.mark.xfail(
    reason="Hueco conocido: el guardarraíl solo mira el prefijo, así que 'SELECT 1; DROP ...' "
           "pasa la validación (en PostgreSQL se ejecutarían ambas sentencias).",
    strict=True,
)
def test_execute_sql_blocks_stacked_statements(engine):
    with pytest.raises(ValueError):
        engine._execute_sql("SELECT 1; DROP TABLE bioreactor_readings")


# ─── Pipeline completo con LLM falso ──────────────────────
def test_chat_happy_path(engine, monkeypatch):
    fake = FakeListLLM(responses=[
        "SELECT * FROM bioreactor_readings WHERE is_anomaly = 1;",
        "One anomaly found at 52°C.",
    ])
    monkeypatch.setattr(engine, "_get_llm", lambda: fake)

    resp = engine.chat("any anomalies?")

    assert resp["error"] is None
    assert resp["row_count"] == 1
    assert resp["answer"] == "One anomaly found at 52°C."
    assert resp["sql"].startswith("SELECT")


def test_chat_reports_security_error_when_llm_returns_write(engine, monkeypatch):
    monkeypatch.setattr(engine, "_get_llm", lambda: FakeListLLM(responses=["DELETE FROM bioreactor_readings;"]))
    resp = engine.chat("wipe everything")
    assert "Security" in resp["error"]
    assert resp["answer"] is None


def test_chat_handles_llm_unavailable(engine, monkeypatch):
    monkeypatch.setattr(engine, "_get_llm", lambda: None)
    resp = engine.chat("hello")
    assert "unavailable" in resp["error"]


def test_chat_handles_invalid_sql(engine, monkeypatch):
    monkeypatch.setattr(engine, "_get_llm", lambda: FakeListLLM(responses=["SELECT * FROM no_such_table;"]))
    resp = engine.chat("broken")
    assert resp["error"] and "Error processing query" in resp["error"]


def test_is_ollama_ready_false_when_service_down(engine, monkeypatch):
    import requests

    def boom(*a, **k):
        raise requests.ConnectionError()

    monkeypatch.setattr(requests, "get", boom)
    assert engine.is_ollama_ready() is False
