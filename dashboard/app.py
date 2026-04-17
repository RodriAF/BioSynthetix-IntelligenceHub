"""
app.py – BioSynthetix Intelligence Hub Dashboard
═════════════════════════════════════════════════
Professional bioreactor monitoring dashboard featuring:
  • Real-time sensor visualization (Plotly) with gap handling
  • Visual anomaly detection (Isolation Forest)
  • Reactor Chat using local LLM (Ollama)
  • Auto-refresh and optimized chat history
"""

import os
import logging
import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text
from streamlit_autorefresh import st_autorefresh

# Internal import (assumed to exist)
from chat.llm_chat import get_chat_engine

# ─── Logging Configuration ────────────────────────────────
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("app")

# ─── Database Connection ──────────────────────────────────
# NOTE: Ensure the DB user has READ-ONLY permissions for securityd 
DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'biosynthetix')}:"
    f"{os.getenv('DB_PASSWORD', 'biosynth_secret')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'bioreactor_db')}"
)


# STREAMLIT CONFIGURATION

st.set_page_config(
    page_title="BioSynthetix Intelligence Hub",
    page_icon="🧬",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ─── Custom CSS ───────────────────────────────────────────
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&family=IBM+Plex+Sans:wght@300;400;600&display=swap');

    html, body, [class*="css"] {
        font-family: 'IBM Plex Sans', sans-serif;
        background-color: #0a0f1a;
        color: #c8d8e8;
    }

    [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1525 0%, #0a1020 100%);
        border-right: 1px solid #1e3a5a;
    }

    [data-testid="stMetric"] {
        background: #0d1a2e;
        border: 1px solid #1e3a5a;
        border-radius: 8px;
        padding: 16px;
    }
    [data-testid="stMetricValue"] {
        color: #4fc3f7 !important;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.8rem !important;
    }
    [data-testid="stMetricLabel"] {
        color: #7ab3cc !important;
        font-size: 0.75rem !important;
        text-transform: uppercase;
        letter-spacing: 0.1em;
    }

    h1, h2, h3 {
        font-family: 'IBM Plex Mono', monospace;
        color: #4fc3f7;
    }

    hr { border-color: #1e3a5a; }
</style>
""", unsafe_allow_html=True)



# DATA FUNCTIONS

@st.cache_resource
def get_engine():
    return create_engine(DB_URL, pool_pre_ping=True)


@st.cache_data(ttl=10)  # Short cache for responsive UI
def load_readings(hours: int = 24) -> pd.DataFrame:
    engine = get_engine()
    query = text("""
        SELECT id, timestamp, temperature_c, ph_level, biomass_g_l,
               dissolved_o2, agitation_rpm, is_anomaly, anomaly_score,
               batch_id, notes
        FROM   bioreactor_readings
        WHERE  timestamp >= NOW() - INTERVAL :interval
        ORDER  BY timestamp ASC
    """)
    try:
        with engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"interval": f"{hours} hours"})
        df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
        return df
    except Exception as e:
        log.error(f"Database error: {e}")
        return pd.DataFrame()


def get_summary_stats(df: pd.DataFrame) -> dict:
    if df.empty:
        return {k: 0 for k in ["total", "anomalies", "temp_last", "ph_last", "biomass_last", "temp_max"]}
    
    return {
        "total":         len(df),
        "anomalies":     int(df["is_anomaly"].sum()),
        "temp_last":     float(df["temperature_c"].iloc[-1]),
        "ph_last":       float(df["ph_level"].iloc[-1]),
        "biomass_last":  float(df["biomass_g_l"].iloc[-1]),
        "temp_max":      float(df["temperature_c"].max()),
    }



# PLOTLY CHARTS

DARK_THEME = dict(
    paper_bgcolor="#0a0f1a",
    plot_bgcolor="#0d1525",
    font=dict(color="#7ab3cc", family="IBM Plex Mono"),
    xaxis=dict(gridcolor="#1e2d40", linecolor="#1e3a5a"),
    yaxis=dict(gridcolor="#1e2d40", linecolor="#1e3a5a"),
)

def build_temperature_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    # Normal Operating Range
    fig.add_hrect(y0=36.0, y1=38.0, fillcolor="rgba(0,230,118,0.06)",
                  line_width=0, annotation_text="Optimal Zone",
                  annotation_font_color="#00e676")

    # Primary series with connectgaps=True
    fig.add_trace(go.Scatter(
        x=df["timestamp"], y=df["temperature_c"],
        mode="lines", name="Temp",
        line=dict(color="#4fc3f7", width=2),
        connectgaps=True 
    ))

    # Anomalies as red markers
    anomalies = df[df["is_anomaly"]]
    if not anomalies.empty:
        fig.add_trace(go.Scatter(
            x=anomalies["timestamp"], y=anomalies["temperature_c"],
            mode="markers", name="Alert",
            marker=dict(color="#ff5252", size=8, symbol="x"),
        ))

    fig.update_layout(title="🌡️ Temperature (°C)", height=300, **DARK_THEME)
    return fig

def build_ph_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp"], y=df["ph_level"],
        mode="lines", name="pH",
        line=dict(color="#ab47bc", width=2),
        fill="tozeroy", fillcolor="rgba(171,71,188,0.05)",
        connectgaps=True
    ))
    fig.update_layout(title="🧪 pH Levels", height=300, **DARK_THEME)
    return fig

def build_biomass_chart(df: pd.DataFrame) -> go.Figure:
    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["timestamp"], y=df["biomass_g_l"],
        mode="lines", name="Biomass",
        line=dict(color="#00e676", width=2),
        connectgaps=True
    ))
    fig.update_layout(title="🦠 Biomass Density (g/L)", height=300, **DARK_THEME)
    return fig


# UI SECTIONS

def render_sidebar(stats: dict, ollama_ready: bool):
    with st.sidebar:
        st.markdown("### BioSynthetix")
        st.markdown("**Intelligence Hub v1.1**")
        st.markdown("---")
        
        st.markdown("#### SYSTEM STATUS")
        st.write(f"DB: `🟢 Connected`" if stats['total'] > 0 else "DB: `🔴 Error`")
        st.write(f"LLM: `{'🟢 Ready' if ollama_ready else '🔴 Offline'}`")
        
        st.markdown("---")
        st.markdown("#### CURRENT PARAMETERS")
        st.metric("Last Temp", f"{stats['temp_last']:.2f} °C")
        st.metric("Last pH", f"{stats['ph_last']:.2f}")
        
        hours = st.slider("Time Window (Hours)", 1, 48, 24)
        return hours

def render_chat_tab(chat_engine):
    st.subheader("💬 AI Reactor Assistant")
    st.info("Ask about trends, anomalies, or summaries. Data remains 100% local.")

    if "chat_history" not in st.session_state:
        st.session_state["chat_history"] = []

    # 1. Dibujar el historial EXISTENTE
    for msg in st.session_state["chat_history"]:
        with st.chat_message(msg["role"], avatar="🧬" if msg["role"] == "assistant" else "👤"):
            st.markdown(msg["content"])

    # 2. Capturar nuevo input
    if user_input := st.chat_input("Ask: 'Was there any anomaly in the last 2 hours?'"):
        # Añadir mensaje del usuario inmediatamente
        st.session_state["chat_history"].append({"role": "user", "content": user_input})
        
        # Mostrar el mensaje del usuario y el spinner
        with st.chat_message("user", avatar="👤"):
            st.markdown(user_input)

        with st.chat_message("assistant", avatar="🧬"):
            with st.spinner("Thinking..."):
                response = chat_engine.chat(user_input)
                answer = response.get("answer", "Error processing request.")
                # Guardar en el historial
                st.session_state["chat_history"].append({"role": "assistant", "content": answer})
                # Limitar historial
                st.session_state["chat_history"] = st.session_state["chat_history"][-10:]
                
                # IMPORTANTE: Forzar recarga para que el bucle de arriba dibuje todo
                st.rerun()


# MAIN EXECUTION

def main():
    # Header
    st.title("BioSynthetix Intelligence Hub")
    st.caption("BIOREACTOR MONITORING • LOCAL-FIRST AI • DATA SOVEREIGNTY")
    
    # Load Engines
    chat_engine = get_chat_engine()
    ollama_ready = chat_engine.is_ollama_ready()
    
    # Initial Data Load
    df = load_readings(24)
    stats = get_summary_stats(df)
    
    # Sidebar
    selected_hours = render_sidebar(stats, ollama_ready)
    if selected_hours != 24:
        df = load_readings(selected_hours)
        stats = get_summary_stats(df)

    # UI Tabs (Improved Chat Location)
    tab_monitor, tab_chat, tab_logs = st.tabs(["📊 Live Monitoring", "💬 AI Assistant", "📑 Anomaly Logs"])

    @st.fragment(run_every=120)
    def render_monitoring_fragment(df, stats):
        kpi1, kpi2, kpi3, kpi4 = st.columns(4)
        kpi1.metric("Temperature", f"{stats['temp_last']:.2f} °C", f"Max: {stats['temp_max']:.1f}")
        kpi2.metric("pH Level", f"{stats['ph_last']:.2f}", "Optimal: 6.8-7.4")
        kpi3.metric("Biomass", f"{stats['biomass_last']:.2f} g/L")
        kpi4.metric("Anomalies", stats['anomalies'], delta_color="inverse" if stats['anomalies']>0 else "normal")

        # Charts Grid
        col_left, col_right = st.columns(2)
        with col_left:
            st.plotly_chart(build_temperature_chart(df), width="stretch")
            st.plotly_chart(build_biomass_chart(df), width="stretch")
        with col_right:
            st.plotly_chart(build_ph_chart(df), width="stretch")
            # Add a Correlation Scatter
            fig_corr = go.Figure(go.Scatter(
                x=df["temperature_c"], y=df["ph_level"], mode="markers",
                marker=dict(color=df["is_anomaly"].map({True: "#ff5252", False: "#4fc3f7"}))
            ))
            fig_corr.update_layout(title="T° vs pH Correlation", height=300, **DARK_THEME)
            st.plotly_chart(fig_corr, width="stretch")

    with tab_monitor:
        render_monitoring_fragment(df, stats)

    with tab_chat:
        render_chat_tab(chat_engine)

    with tab_logs:
        st.subheader("Detected Anomaly History")
        anomalies_only = df[df["is_anomaly"]].sort_values("timestamp", ascending=False)
        if not anomalies_only.empty:
            st.dataframe(anomalies_only, width="stretch", hide_index=True)
        else:
            st.success("No anomalies detected in the selected time window.")

    # Footer
    st.markdown("---")
    st.markdown("<div style='text-align:center; font-family:monospace; font-size:0.7rem; color:#4a6a8a'>"
                "SECURE LOCAL MONITORING SYSTEM • AUTO-REFRESH ENABLED • BUILD V1.1</div>", 
                unsafe_allow_html=True)

if __name__ == "__main__":
    main()