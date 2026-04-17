"""
llm_chat.py – Local LLM Chat Engine (LangChain + Ollama) + Text-to-SQL
════════════════════════════════════════════════════════════════════
Fully local conversational intelligence engine.
No data leaves the internal infrastructure.

Workflow:
  Natural Language Question
        ↓
  Local LLM (Ollama/mistral) generates SQL
        ↓
  Executes SQL in PostgreSQL
        ↓
  LLM interprets results and responds in English
"""

import os
import logging
import re
from typing import Optional

from langchain_community.llms import Ollama
from langchain_core.prompts import PromptTemplate
from langchain_core.output_parsers import StrOutputParser
from sqlalchemy import create_engine, text

log = logging.getLogger("llm_chat")

# ─── Configuration ────────────────────────────────────────
DB_URL = (
    f"postgresql://{os.getenv('DB_USER', 'biosynthetix')}:"
    f"{os.getenv('DB_PASSWORD', 'biosynth_secret')}@"
    f"{os.getenv('DB_HOST', 'localhost')}:"
    f"{os.getenv('DB_PORT', '5432')}/"
    f"{os.getenv('DB_NAME', 'bioreactor_db')}"
)

OLLAMA_BASE_URL = os.getenv("OLLAMA_BASE_URL", "http://localhost:11434")
OLLAMA_MODEL    = os.getenv("OLLAMA_MODEL", "mistral")



# 1. DATABASE SCHEMA (Context for the LLM)


SCHEMA_DESCRIPTION = """
TABLE: bioreactor_readings
DESCRIPTION: Real-time bioreactor sensor readings.

COLUMNS:
  id              INTEGER       → Unique identifier
  timestamp       TIMESTAMPTZ   → Date and time of reading (with timezone)
  temperature_c   NUMERIC       → Temperature in Celsius (Normal range: 36-38°C)
  ph_level        NUMERIC       → pH Level (Normal range: 6.8-7.4)
  biomass_g_l     NUMERIC       → Biomass concentration in g/L (Normal range: 1-15)
  dissolved_o2    NUMERIC       → Dissolved Oxygen % (Normal range: 20-60%)
  agitation_rpm   INTEGER       → Agitation speed in RPM
  is_anomaly      BOOLEAN       → TRUE if flagged by Isolation Forest as an anomaly
  anomaly_score   NUMERIC       → Anomaly score (more negative = more anomalous)
  batch_id        VARCHAR       → Batch identifier (e.g., 'BATCH-001')
  notes           TEXT          → System notes or automated alerts

VALID QUERY EXAMPLES:
  -- Anomalies in the last 5 hours:
  SELECT * FROM bioreactor_readings 
  WHERE is_anomaly = TRUE AND timestamp >= NOW() - INTERVAL '5 hours';

  -- Hourly average temperature:
  SELECT DATE_TRUNC('hour', timestamp) as hour, AVG(temperature_c) as avg_temp
  FROM bioreactor_readings GROUP BY hour ORDER BY hour DESC;

  -- Last 10 readings:
  SELECT * FROM bioreactor_readings ORDER BY timestamp DESC LIMIT 10;
"""



# 2. PROMPT TEMPLATES


SQL_GENERATION_PROMPT = PromptTemplate(
    input_variables=["schema", "question"],
    template="""You are an expert SQL Analyst for industrial bioengineering systems.
Your task is to convert natural language questions into valid PostgreSQL queries.

DATABASE SCHEMA:
{schema}

CRITICAL RULES:
1. Respond ONLY with the SQL query. No explanations, no extra text.
2. Do NOT use code blocks (No ```).
3. Always use "NOW()" for current time references.
4. For time intervals, use the syntax: NOW() - INTERVAL '5 hours'
5. If the user asks for "anomalies", use: WHERE is_anomaly = TRUE
6. Limit results using LIMIT where appropriate (Max 50 rows).
7. For date ranges, use BETWEEN or >= / <= operators.

USER QUESTION: {question}

SQL (Query only):"""
)

ANSWER_GENERATION_PROMPT = PromptTemplate(
    input_variables=["question", "sql_query", "sql_results", "row_count"],
    template="""You are the Intelligence Assistant of the BioSynthetix Reactor Hub.
Analyze the database results and provide a clear, technical response in English.

ORIGINAL QUESTION: {question}
EXECUTED SQL: {sql_query}
ROWS RETURNED: {row_count}
DATA RESULTS: {sql_results}

INSTRUCCIONES:
- If anomalies are present, highlight them with urgency and specify out-of-range values.
- Provide relevant technical context (normal ranges, possible biological causes).
- If no data is found, state it clearly.
- Be concise yet thorough. Use bullet points for multiple findings.
- Respond in technical/scientific English.

ASSISTANT RESPONSE:"""
)



# 3. MAIN CLASS: BioReactorChatEngine


class BioReactorChatEngine:
    """
    Local chat engine combining:
    - Ollama (Local LLM) for SQL generation and interpretation
    - PostgreSQL for data execution
    - LangChain for prompt orchestration
    """

    def __init__(self):
        self.engine = create_engine(DB_URL, pool_pre_ping=True)
        self._llm   = None
        self._ready = False

    def _get_llm(self) -> Optional[Ollama]:
        """Initializes the Ollama LLM with increased timeout."""
        if self._llm is None:
            try:
                self._llm = Ollama(
                    base_url=OLLAMA_BASE_URL,
                    model=OLLAMA_MODEL,
                    temperature=0.1,
                    timeout=180, 
                )
                self._ready = True
                log.info(f"[SUCCESS]: Local LLM connected: {OLLAMA_MODEL}")
            except Exception as e:
                log.error(f"[ERROR]: Error connecting to Ollama: {e}")
                self._ready = False
        return self._llm

    def is_ollama_ready(self) -> bool:
        """Checks if the Ollama service and model are available."""
        import requests
        try:
            r = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5)
            models = r.json().get("models", [])
            return any(OLLAMA_MODEL in m.get("name", "") for m in models)
        except Exception:
            return False

    def _extract_sql(self, raw_text: str) -> str:
        """Cleans the SQL response from the LLM."""
        clean = re.sub(r"```sql\s*", "", raw_text, flags=re.IGNORECASE)
        clean = re.sub(r"```\s*", "", clean)
        clean = clean.strip()

        # Regex to capture only the query part starting with SELECT or WITH
        match = re.search(
            r"(SELECT|WITH|INSERT|UPDATE|DELETE).*?;",
            clean,
            flags=re.IGNORECASE | re.DOTALL,
        )
        if match:
            return match.group(0).strip()
        return clean

    def _execute_sql(self, sql: str) -> tuple[list[dict], int]:
        """Executes SQL and returns results as a list of dictionaries."""
        sql_upper = sql.strip().upper()
        # Security Guardrail: Only allow read-only operations
        if not sql_upper.startswith("SELECT") and not sql_upper.startswith("WITH"):
            raise ValueError("Security restriction: Only SELECT queries are allowed.")

        with self.engine.connect() as conn:
            result = conn.execute(text(sql))
            rows = [dict(zip(result.keys(), row)) for row in result.fetchall()]
        return rows, len(rows)

    def chat(self, user_question: str) -> dict:
        """
        Full Text-to-SQL Pipeline:
        1. Generate SQL from question
        2. Execute SQL
        3. Interpret and respond in natural language
        
        Returns: dict with sql, results, answer, error
        """
        response = {
            "question": user_question,
            "sql":      None,
            "results":  None,
            "answer":   None,
            "row_count": 0,
            "error":    None,
        }

        llm = self._get_llm()
        if llm is None:
            response["error"] = (
                "[ERROR]: Local LLM is unavailable. "
                "Ensure Ollama is running and the model is downloaded."
            )
            return response

        try:
            # ── STEP 1: SQL Generation ──────────────────
            log.info(f"[QUESTION]: User question: {user_question}")
            sql_chain = SQL_GENERATION_PROMPT | llm | StrOutputParser()
            raw_sql = sql_chain.invoke({
                "schema":   SCHEMA_DESCRIPTION,
                "question": user_question,
            })

            sql = self._extract_sql(raw_sql)
            response["sql"] = sql
            log.info(f"[PROCESS]: Generated SQL: {sql}")

            # ── STEP 2: SQL Execution ───────────────────
            rows, row_count = self._execute_sql(sql)
            response["results"]   = rows
            response["row_count"] = row_count
            log.info(f"[TABLE]: {row_count} rows returned.")

            # ── STEP 3: Natural Language Interpretation ──
            # Limit results string to avoid context overflow
            results_str = "\n".join(
                [str(row) for row in rows[:20]] 
            ) if rows else "No results found"

            answer_chain = ANSWER_GENERATION_PROMPT | llm | StrOutputParser()
            answer = answer_chain.invoke({
                "question":    user_question,
                "sql_query":   sql,
                "sql_results": results_str,
                "row_count":   row_count,
            })
            response["answer"] = answer
            log.info(f"[SUCCESS]: Response generated by local LLM: {answer}")

        except ValueError as e:
            response["error"] = f"[WARNING]: Security Restriction: {e}"
        except Exception as e:
            log.error(f"Pipeline error: {e}")
            response["error"] = f"[ERROR]: Error processing query: {str(e)}"

        return response


# ─── Global Singleton Instance ────────────────────────────
_chat_engine: Optional[BioReactorChatEngine] = None

def get_chat_engine() -> BioReactorChatEngine:
    global _chat_engine
    if _chat_engine is None:
        _chat_engine = BioReactorChatEngine()
    return _chat_engine