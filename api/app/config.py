"""
Centralized configuration — single source of truth for model names, URLs, and retry budgets.

All services import from here instead of reading os.environ directly,
making configuration testable and eliminating duplication.
"""

import os

# ── Ollama connection ────────────────────────────────────────────────────────

OLLAMA_BASE_URL = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")

# ── Model identifiers ────────────────────────────────────────────────────────

OLLAMA_MODEL = os.environ.get(
    "OLLAMA_MODEL",
    "hf.co/mradermacher/Arctic-Text2SQL-R1-7B-GGUF:Q4_K_M",
)
OLLAMA_MODEL_BASE = os.environ.get("OLLAMA_MODEL_BASE", "qwen2.5-coder:3b")
ANSWER_MODEL = os.environ.get("ANSWER_MODEL", "llama3.2:1b")
ANSWER_MODEL_BIG = os.environ.get("ANSWER_MODEL_BIG", "llama3.2:3b")

ALL_MODELS = [OLLAMA_MODEL, OLLAMA_MODEL_BASE, ANSWER_MODEL, ANSWER_MODEL_BIG]

# ── Retry budgets ─────────────────────────────────────────────────────────────

BASE_ATTEMPTS = 3          # Qwen attempts before Arctic escalation (SIMPLE path)
ARCTIC_ATTEMPTS = 2        # Arctic attempts after Qwen exhausted (SIMPLE path)
MAX_ATTEMPTS = BASE_ATTEMPTS + ARCTIC_ATTEMPTS  # Total attempts for SIMPLE path
ARCTIC_DIRECT_ATTEMPTS = 5  # Arctic attempts when HARD complexity is detected

# ── Per-model Ollama generation options ───────────────────────────────────────
# Arctic needs 2048 num_predict for CoT reasoning; Qwen only needs ~512 for SQL.
# Qwen stop sequences prevent document-continuation after the first ```sql block.

MODEL_OPTIONS: dict[str, dict] = {
    OLLAMA_MODEL: {
        "num_ctx": 4096,
        "num_predict": 2048,
    },
    OLLAMA_MODEL_BASE: {
        "num_ctx": 4096,
        "num_predict": 512,
        "stop": ["\n```"],
    },
}
