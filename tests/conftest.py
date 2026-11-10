"""Fixtures compartidas: una BD SQLite en memoria que sustituye a PostgreSQL."""
import pytest
from sqlalchemy import create_engine, text
from sqlalchemy.pool import StaticPool


# Subconjunto portable del esquema de DB/init_db.sql
SQLITE_SCHEMA = """
CREATE TABLE bioreactor_readings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp     TIMESTAMP NOT NULL,
    temperature_c REAL NOT NULL,
    ph_level      REAL NOT NULL,
    biomass_g_l   REAL NOT NULL,
    dissolved_o2  REAL,
    agitation_rpm INTEGER,
    is_anomaly    BOOLEAN DEFAULT 0,
    anomaly_score REAL,
    batch_id      VARCHAR(20) DEFAULT 'BATCH-001',
    notes         TEXT
)
"""


@pytest.fixture
def sqlite_engine():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    with engine.begin() as conn:
        conn.execute(text(SQLITE_SCHEMA))
    yield engine
    engine.dispose()
