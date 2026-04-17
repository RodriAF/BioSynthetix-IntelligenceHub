-- ═══════════════════════════════════════════════════════════
--  BioSynthetix Intelligence Hub – Initial Schema
--  Automatically executed by PostgreSQL during initialization
-- ═══════════════════════════════════════════════════════════

-- Main table for bioreactor sensor readings
CREATE TABLE IF NOT EXISTS bioreactor_readings (
    id              SERIAL PRIMARY KEY,
    timestamp       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    temperature_c   NUMERIC(6, 2) NOT NULL,   -- °C  (normal range: 36.0 – 38.0)
    ph_level        NUMERIC(4, 2) NOT NULL,    -- pH  (normal range: 6.8 – 7.4)
    biomass_g_l     NUMERIC(8, 3) NOT NULL,    -- g/L (normal range: 1.0 – 15.0)
    dissolved_o2    NUMERIC(5, 2),             -- %   (normal range: 20 – 60)
    agitation_rpm   INTEGER,                   -- RPM (normal range: 100 – 400)
    is_anomaly      BOOLEAN DEFAULT FALSE,
    anomaly_score   NUMERIC(8, 6),             -- Isolation Forest score (negative = anomaly)
    batch_id        VARCHAR(20) DEFAULT 'BATCH-001',
    notes           TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW()
);

-- Indices to optimize time-series queries (critical for Text-to-SQL)
CREATE INDEX IF NOT EXISTS idx_timestamp ON bioreactor_readings (timestamp DESC);
CREATE INDEX IF NOT EXISTS idx_anomaly   ON bioreactor_readings (is_anomaly);
CREATE INDEX IF NOT EXISTS idx_batch     ON bioreactor_readings (batch_id);

-- Convenient view for the LLM (simplifies the exposed schema)
CREATE OR REPLACE VIEW v_sensor_summary AS
SELECT
    DATE_TRUNC('hour', timestamp) AS hour,
    batch_id,
    ROUND(AVG(temperature_c)::NUMERIC, 2)  AS avg_temp_c,
    ROUND(AVG(ph_level)::NUMERIC, 3)       AS avg_ph,
    ROUND(AVG(biomass_g_l)::NUMERIC, 3)    AS avg_biomass_g_l,
    COUNT(*)                                AS total_readings,
    SUM(CASE WHEN is_anomaly THEN 1 ELSE 0 END) AS total_anomalies
FROM bioreactor_readings
GROUP BY DATE_TRUNC('hour', timestamp), batch_id
ORDER BY hour DESC;

-- Column comments (useful for Text-to-SQL with introspection)
COMMENT ON TABLE  bioreactor_readings IS 'Real-time bioreactor sensor readings';
COMMENT ON COLUMN bioreactor_readings.temperature_c  IS 'Temperature in degrees Celsius. Normal: 36-38°C';
COMMENT ON COLUMN bioreactor_readings.ph_level       IS 'pH level. Normal: 6.8-7.4';
COMMENT ON COLUMN bioreactor_readings.biomass_g_l    IS 'Biomass concentration in g/L. Normal: 1-15 g/L';
COMMENT ON COLUMN bioreactor_readings.is_anomaly     IS 'TRUE if Isolation Forest flagged this reading as anomalous';
COMMENT ON COLUMN bioreactor_readings.anomaly_score  IS 'Anomaly score: more negative values = higher abnormality';