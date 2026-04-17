"""
ingest_data.py – Ingestion Module with Pydantic Validation
═══════════════════════════════════════════════════════════
Generates synthetic bioreactor data, validates it using Pydantic,
and inserts it into PostgreSQL. Includes a deliberate temperature
anomaly to demonstrate Isolation Forest detection.
"""

import os
import random
import logging
from datetime import datetime, timedelta, timezone
from typing import Optional

import numpy as np
from pydantic import BaseModel, Field, field_validator, ValidationError
from sqlalchemy import create_engine, text

# ─── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("ingest_data")

# ─── Database Configuration ───────────────────────────────
DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'biosynthetix')}:"
    f"{os.getenv('DB_PASSWORD', 'biosynth_secret')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'bioreactor_db')}"
)



# 1.  PYDANTIC MODELS – Data Contract

class BioreactorReading(BaseModel):
    """
    Validation model for a bioreactor reading.
    Pydantic ensures correct types BEFORE touching the database.
    """
    timestamp: datetime
    temperature_c: float = Field(
        ...,
        description="Temperature in °C",
        ge=0.0,    # greater than or equal to 0
        le=100.0,  # less than or equal to 100
    )
    ph_level: float = Field(
        ...,
        description="pH Level",
        ge=0.0,
        le=14.0,
    )
    biomass_g_l: float = Field(
        ...,
        description="Biomass in g/L",
        ge=0.0,
        le=100.0,
    )
    dissolved_o2: Optional[float] = Field(
        None,
        description="Dissolved Oxygen %",
        ge=0.0,
        le=100.0,
    )
    agitation_rpm: Optional[int] = Field(
        None,
        description="Agitation in RPM",
        ge=0,
        le=1500,
    )
    batch_id: str = Field(default="BATCH-001", max_length=20)
    notes: Optional[str] = Field(None, max_length=500)

    @field_validator("temperature_c")
    @classmethod
    def warn_temperature_range(cls, v: float) -> float:
        """Validates normal operating range (logs warning, does not block)."""
        if not (34.0 <= v <= 40.0):
            log.warning(
                f"[BIOREACTOR WARNING]: Temperature outside standard operating range: {v}°C "
                f"(expected: 34-40°C)"
            )
        return round(v, 2)

    @field_validator("ph_level")
    @classmethod
    def validate_ph_range(cls, v: float) -> float:
        if not (5.0 <= v <= 9.0):
            log.warning(f"[BIOREACTOR WARNING]: pH outside operating range: {v}")
        return round(v, 2)

    @field_validator("biomass_g_l")
    @classmethod
    def validate_biomass(cls, v: float) -> float:
        return round(v, 3)



# 2.  SYNTHETIC DATA GENERATOR

def generate_bioreactor_data(
    hours: int = 24,
    readings_per_hour: int = 6,
    batch_id: str = "BATCH-001",
) -> list[dict]:
    """
    Generates realistic bioreactor data for the last `hours` hours.
    Includes a deliberate temperature anomaly at -6h.
    """
    random.seed(42)
    np.random.seed(42)

    now = datetime.now(tz=timezone.utc)
    records = []

    total_readings = hours * readings_per_hour
    log.info(f"[PROCESS]: Generating {total_readings} readings for batch {batch_id}...")

    for i in range(total_readings):
        ts = now - timedelta(minutes=(total_readings - i) * (60 / readings_per_hour))

        # Base parameters with realistic Gaussian noise
        temperature = np.random.normal(loc=37.0, scale=0.3)
        ph          = np.random.normal(loc=7.1,  scale=0.05)
        biomass     = max(0.5, 2.0 + (i / total_readings) * 10 + np.random.normal(0, 0.2))
        dissolved_o2 = np.random.normal(loc=40.0, scale=3.0)
        rpm          = random.randint(150, 250)

        # ★ DELIBERATE ANOMALY: Temperature spike approx. 6 hours ago
        #   Simulates a cooling system failure
        anomaly_window_start = total_readings - (6 * readings_per_hour) - 3
        anomaly_window_end   = total_readings - (6 * readings_per_hour) + 3
        note = None

        if anomaly_window_start <= i <= anomaly_window_end:
            temperature = np.random.normal(loc=52.0, scale=1.5)  # ← Spike!
            note = "BIOREACTOR WARNING: Anomalous temperature detected - possible cooling failure"
            log.info(f" [PROCESS]: Inserting temperature anomaly at index {i}: {temperature:.2f}°C")

        raw_record = {
            "timestamp":     ts,
            "temperature_c": temperature,
            "ph_level":      ph,
            "biomass_g_l":   biomass,
            "dissolved_o2":  min(100.0, max(0.0, dissolved_o2)),
            "agitation_rpm": rpm,
            "batch_id":      batch_id,
            "notes":         note,
        }

        # ── Pydantic Validation ──────────────────────────
        try:
            validated = BioreactorReading(**raw_record)
            records.append(validated.model_dump())
        except ValidationError as e:
            log.error(f"[ERROR]: Invalid record at index {i}: {e}")
            continue

    log.info(f"[SUCCESS]: {len(records)} records successfully validated.")
    return records



# 3.  POSTGRESQL INSERTION

def insert_records(records: list[dict]) -> None:
    """Inserts validated records into PostgreSQL using SQLAlchemy."""
    engine = create_engine(DB_URL, pool_pre_ping=True)

    insert_sql = text("""
        INSERT INTO bioreactor_readings
            (timestamp, temperature_c, ph_level, biomass_g_l,
             dissolved_o2, agitation_rpm, batch_id, notes)
        VALUES
            (:timestamp, :temperature_c, :ph_level, :biomass_g_l,
             :dissolved_o2, :agitation_rpm, :batch_id, :notes)
    """)

    with engine.begin() as conn:
        # Clear previous data for reproducible demo
        conn.execute(text("DELETE FROM bioreactor_readings"))
        log.info("[PROCESS]: Table cleared for reproducible demo.")

        conn.execute(insert_sql, records)
        log.info(f"[SUCCESS]: {len(records)} records inserted into PostgreSQL.")



# 4.  ENTRYPOINT

if __name__ == "__main__":
    log.info("═" * 55)
    log.info("  BioSynthetix – Data Ingestion Module")
    log.info("═" * 55)

    data = generate_bioreactor_data(hours=24, readings_per_hour=6)
    insert_records(data)

    log.info("[SUCCESS]: Ingestion completed.")