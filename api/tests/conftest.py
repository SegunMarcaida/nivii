"""
Root conftest.py — shared configuration for all tests.
Sets DATABASE_URL env var so app modules can import without raising KeyError.
"""
import os

# Provide a default DATABASE_URL so app.db.database imports don't fail at collection time.
# Integration tests override this via the integration/conftest.py fixtures.
os.environ.setdefault(
    "DATABASE_URL",
    "sqlite+aiosqlite:///./data/confectionery.db",
)
os.environ.setdefault("OLLAMA_BASE_URL", "http://localhost:11434")
os.environ.setdefault("OLLAMA_MODEL", "hf.co/mradermacher/Arctic-Text2SQL-R1-7B-GGUF:Q4_K_M")
os.environ.setdefault("OLLAMA_MODEL_BASE", "qwen2.5-coder:3b")
