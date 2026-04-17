"""
detect_anomalies.py – Anomaly Detection with Isolation Forest
══════════════════════════════════════════════════════════════════
Reads bioreactor data from PostgreSQL, trains a Scikit-Learn 
Isolation Forest model, and updates the is_anomaly column 
in the database. 100% local, no external APIs.
"""

import os
import logging

import numpy as np
import pandas as pd
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from sqlalchemy import create_engine, text

# ─── Logging ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger("detect_anomalies")

# ─── Connection ───────────────────────────────────────────
DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'biosynthetix')}:"
    f"{os.getenv('DB_PASSWORD', 'biosynth_secret')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'bioreactor_db')}"
)

# ─── Model Parameters ──────────────────────────────────────
CONTAMINATION = 0.04   # ~4% expected outliers
RANDOM_STATE  = 42
FEATURES      = ["temperature_c", "ph_level", "biomass_g_l", "dissolved_o2"]



# 1.  DATA LOADING

def load_data(engine) -> pd.DataFrame:
    query = """
        SELECT id, temperature_c, ph_level, biomass_g_l, dissolved_o2
        FROM   bioreactor_readings
        WHERE  dissolved_o2 IS NOT NULL
        ORDER  BY timestamp ASC
    """
    df = pd.read_sql(query, engine)
    log.info(f"[PROCESS]: Loaded {len(df)} readings from PostgreSQL.")
    return df



# 2.  TRAINING AND PREDICTION

def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trains Isolation Forest on bioreactor features.
    
    Why Isolation Forest?
    - Unsupervised (no labels needed) → ideal for industrial data
    - Robust to non-normal distributions
    - Efficient in high dimensionality
    - Interpretable: the score indicates "how anomalous" a point is
    """
    X = df[FEATURES].copy()

    # Normalization: IF doesn't strictly require this, but it improves interpretability
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    log.info(f"[PROCESS]: Training Isolation Forest (contamination={CONTAMINATION})...")

    model = IsolationForest(
        n_estimators=200,        # More trees = higher stability
        contamination=CONTAMINATION,
        max_samples="auto",
        random_state=RANDOM_STATE,
        n_jobs=-1,               # Full parallelization
    )
    model.fit(X_scaled)

    # predict: -1 = anomaly, 1 = normal
    predictions = model.predict(X_scaled)
    scores      = model.score_samples(X_scaled)  # More negative = more anomalous

    df = df.copy()
    df["is_anomaly"]    = predictions == -1
    df["anomaly_score"] = scores

    n_anomalies = df["is_anomaly"].sum()
    log.info(
        f"[SUCCESS]: Detection complete: {n_anomalies} anomalies found "
        f"({n_anomalies/len(df)*100:.1f}% of total)"
    )

    # Log anomaly details
    anomalies_df = df[df["is_anomaly"]]
    for _, row in anomalies_df.iterrows():
        log.warning(
            f"  [BIOREACTOR WARNING]:  ID={row['id']:5d} | "
            f"T={row['temperature_c']:.1f}°C | "
            f"pH={row['ph_level']:.2f} | "
            f"BM={row['biomass_g_l']:.2f} g/L | "
            f"Score={row['anomaly_score']:.4f}"
        )

    return df



# 3.  WRITING RESULTS TO POSTGRES

def update_anomaly_flags(df: pd.DataFrame, engine) -> None:
    """Bulk updates anomaly flags in PostgreSQL."""
    anomaly_data = df[["id", "is_anomaly", "anomaly_score"]].to_dict("records")

    update_sql = text("""
        UPDATE bioreactor_readings
        SET    is_anomaly    = :is_anomaly,
               anomaly_score = :anomaly_score
        WHERE  id = :id
    """)

    with engine.begin() as conn:
        conn.execute(update_sql, anomaly_data)

    log.info(f"[SUCCESS]: Updated {len(anomaly_data)} records in PostgreSQL.")



# 4.  STATISTICS REPORTING

def print_report(df: pd.DataFrame) -> None:
    log.info("─" * 55)
    log.info("  ANOMALY DETECTION REPORT")
    log.info("─" * 55)
    log.info(f"  Total readings analyzed  : {len(df)}")
    log.info(f"  Anomalies detected       : {df['is_anomaly'].sum()}")
    log.info(f"  Anomaly rate             : {df['is_anomaly'].mean()*100:.2f}%")

    if df["is_anomaly"].any():
        anomalies = df[df["is_anomaly"]]
        log.info(f"  Avg Temp in anomalies    : {anomalies['temperature_c'].mean():.2f}°C")
        log.info(f"  Min score (most extreme) : {anomalies['anomaly_score'].min():.4f}")
    log.info("─" * 55)



# 5.  ENTRYPOINT

if __name__ == "__main__":
    log.info("═" * 55)
    log.info("  BioSynthetix – Anomaly Detection (IF)")
    log.info("═" * 55)

    engine = create_engine(DB_URL, pool_pre_ping=True)

    df      = load_data(engine)
    df_out  = detect_anomalies(df)
    update_anomaly_flags(df_out, engine)
    print_report(df_out)

    log.info("[SUCCESS]: Anomaly detection task completed.")