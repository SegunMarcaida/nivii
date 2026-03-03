"""
Query complexity classification and on-topic detection.

classify_complexity() routes SIMPLE queries to Qwen and HARD queries to Arctic.
is_on_topic() guards against off-topic questions before any LLM call.
"""

import re
import unicodedata
from enum import Enum


class QueryComplexity(str, Enum):
    SIMPLE = "simple"
    HARD   = "hard"


def QUESTION_NORMALIZATION(question: str) -> str:
    """Normalize text for deterministic rule-based intent matching."""
    normalized = unicodedata.normalize("NFKD", question)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9%]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


_HARD_INTENT_PATTERNS: tuple[re.Pattern[str], ...] = (
    # Explicit window / analytical function keywords.
    # "ranking" implies positional numbers (ROW_NUMBER), not just ORDER BY.
    re.compile(
        r"\b("
        r"rank(?:ing|ed)?|window|partition|row_number|dense_rank|ntile|decile|quartile|"
        r"running total|cumulative|rolling|moving average"
        r")\b"
    ),
    # Nth-order ranking — requires subquery or window function.
    re.compile(
        r"\b("
        r"second|third|fourth|segundo|tercer|cuarto"
        r")\s+("
        r"highest|lowest|top|bottom|best|worst|mas|menos"
        r")\b"
    ),
    # Per-group top-N: "top X per/each/every [group]" — requires ROW_NUMBER OVER PARTITION.
    # Simple "top 5 products" or "highest revenue" (no group qualifier) routes to Qwen.
    re.compile(
        r"\b(top|bottom|best|worst|highest|lowest|most|least)\b.{0,30}\b(per|each|every|por cada)\b"
    ),
    # Period-over-period WITH explicit time reference — requires self-join, LAG, or multi-CTE.
    # Plain "show monthly totals" (GROUP BY month) routes to Qwen.
    re.compile(
        r"\b("
        r"month over month|week over week|year over year|"
        r"mes a mes|semana a semana|"
        r"crecimiento"
        r")\b"
        r"|\b(vs|versus|growth|change|compared?)\b.{0,20}\b(last|previous|prior|anterior|pasada|pasado)\b"
    ),
    # True anti-join: items ABSENT from a period — requires NOT IN or LEFT JOIN IS NULL.
    # "sales without credit notes" (WHERE flag=0) is SIMPLE and routes to Qwen.
    re.compile(
        r"\b(never|not sold|no sales|nunca|nunca vendido|sin ventas)\b"
    ),

    # Date grouping — requires strftime() for monthly/weekly buckets.
    re.compile(
        r"\b(monthly|weekly|by month|by week|per month|per week|each month|each week)\b"
    ),

    # Time-of-day analysis — requires SUBSTR(sale_hour, 1, 2) for grouping or range filter.
    # Note: QUESTION_NORMALIZATION strips colons, so "12:00" becomes "12 00" in normalized form.
    re.compile(
        r"\b(by hour|per hour|busiest hour|peak hour|rush hour|hourly|hour|"
        r"time block|time of day|morning|afternoon|evening|night)\b"
        r"|\b\d{1,2} \d{2}\b.{0,20}\b\d{1,2} \d{2}\b"
    ),

    # Standard deviation — SQLite has no STDDEV(); needs SQRT(AVG(x*x)-AVG(x)*AVG(x)).
    re.compile(
        r"\b(standard deviation|std dev|stddev|variability|volatility)\b"
    ),

    # Growth rate / acceleration — requires comparing two sequential rates.
    # No trailing \b so "accelerat" matches "accelerate", "accelerating", "acceleration".
    re.compile(
        r"\b(growth rate|rate of growth|accelerat|decelerat|tasa de crecimiento|"
        r"fastest grow|slowest grow)"
    ),
)


def classify_complexity(question: str) -> QueryComplexity:
    """
    Rule-based complexity classification used for model routing and telemetry.

    HARD routes directly to Arctic-Text2SQL-R1-7B. Covers advanced analytics
    (ranking/window, anti-join, period-over-period), plus medium-complexity patterns
    that require strftime(), SUBSTR(sale_hour), stddev, or growth-rate math.
    Plain filters and simple aggregations default to SIMPLE (Qwen first).
    """
    q_normalized = QUESTION_NORMALIZATION(question)
    for pattern in _HARD_INTENT_PATTERNS:
        if pattern.search(q_normalized):
            return QueryComplexity.HARD
    return QueryComplexity.SIMPLE


_ON_TOPIC_KEYWORDS = {
    "sale", "sales", "revenue", "product", "products", "waiter", "waiters",
    "ticket", "tickets", "invoice", "quantity", "price", "total",
    "week", "month", "day", "hour", "best", "top", "bottom", "most",
    "least", "cashier", "vendedor", "factura", "nota", "credito",
    "earn", "sold", "item", "never", "return", "purchase",
    "category", "categoria", "packaging", "unit",
}


def is_on_topic(question: str) -> bool:
    """Return True if the question appears to be about sales/POS data."""
    q_lower = question.lower()
    return any(kw in q_lower for kw in _ON_TOPIC_KEYWORDS)
