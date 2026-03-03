"""
few_shot.py — Arctic-Text2SQL-R1-7B prompt system for the flat SQLite sales table.

Implements the official Arctic prompt template (arXiv:2505.20315, Appendix C).
The model outputs reasoning inside <think> tags and the final SQL inside <answer> tags.
Business rules and 5 targeted few-shot examples are injected into the question field.
"""

import re
import unicodedata
from enum import Enum
from typing import Optional


# ── Complexity classification ─────────────────────────────────────────────────
# Retained for telemetry and future routing decisions.

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


# ── Enriched DDL ──────────────────────────────────────────────────────────────
# Column comments include representative values, format examples, and semantic
# descriptions. OmniSQL was trained to use these -- comments as context.

SALES_DDL = """
-- Each row represents one line item from a POS ticket in an Argentine confectionery.
-- One ticket may contain many rows (one per product sold).
CREATE TABLE sales (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,

    -- DATE / TIME (stored as ISO TEXT for strftime() compatibility)
    sale_date           TEXT NOT NULL,   -- Format: YYYY-MM-DD. Examples: '2024-09-22', '2024-10-15', '2024-11-01'
    week_day            TEXT NOT NULL,   -- Full English name. Values: 'Monday','Tuesday','Wednesday','Thursday','Friday','Saturday','Sunday'
    sale_hour           TEXT NOT NULL,   -- 24h HH:MM format. Examples: '09:00', '14:30', '20:15'

    -- TICKET IDENTIFICATION
    ticket_number       TEXT NOT NULL,   -- Full POS code. Examples: 'FCB 0003-000024735', 'NCB 0001-000000043'
    ticket_type         TEXT NOT NULL,   -- Values: 'FCB' (B2C invoice), 'FCA' (B2B invoice), 'NCB' (B2C credit note), 'NCA' (B2B credit note)

    -- PERSONNEL
    waiter              INTEGER,         -- Cashier/waiter numeric ID (integer only; NO waiter_name column). Values: 0 (self-service/no waiter), 51, 52, 101, 102, 103, 104, 105, 116. Exclude self-service: waiter != 0

    -- PRODUCT
    product_name        TEXT NOT NULL,   -- Product description. Examples: 'Alfajor mixto caja x12un', 'Conito x un', 'Alfajor Super DDL x un'. Special value: 'ART. INEXISTENTE' = manual POS adjustment.

    -- AMOUNTS (all in Argentine Pesos ARS)
    quantity            REAL NOT NULL,   -- Units sold on this line. Negative for credit note returns (e.g. -1.0). Can be fractional (e.g. 0.5).
    unitary_price       REAL NOT NULL,   -- Price per unit. 0.0 means promotional sample (free giveaway, no revenue).
    total               REAL NOT NULL,   -- Always equals quantity * unitary_price exactly. Use this column for revenue, not quantity*unitary_price.

    -- PRE-COMPUTED BUSINESS RULE FLAGS (use in WHERE clauses for reliability)
    is_credit_note      INTEGER NOT NULL DEFAULT 0,  -- 1 if ticket_type IN ('NCB','NCA'), else 0
    is_promotional      INTEGER NOT NULL DEFAULT 0,  -- 1 if unitary_price = 0.0, else 0
    is_manual_adj       INTEGER NOT NULL DEFAULT 0   -- 1 if product_name = 'ART. INEXISTENTE', else 0
);
"""


# ── Business rules ────────────────────────────────────────────────────────────
# Appended to the question field (official OmniSQL guidance: external knowledge
# goes in the question placeholder, not as extra prompt sections).

BUSINESS_RULES = """
--- BUSINESS RULES (apply to every query unless explicitly asked otherwise) ---

STANDARD REVENUE FILTER:
  Always add: WHERE is_credit_note = 0 AND is_promotional = 0 AND is_manual_adj = 0
  This excludes returns, free giveaways, and manual POS adjustments.

REVENUE DEFINITION:
  Revenue = SUM(total). Never compute quantity * unitary_price — use the pre-computed total column.

WAITERS:
  The `waiter` column is an INTEGER ID — it is the ONLY cashier/employee identifier.
  There is NO waiter_name, employee_name, or cashier_name column in this schema; never reference them.
  waiter = 0 means self-service (no cashier assigned). Exclude self-service: AND waiter != 0
  Include waiter = 0 only when the question explicitly asks about all sales regardless of cashier.

DATE RANGES AND FORMAT:
  Data spans: 2024-09-21 to 2024-11-20 (3 months: September, October, November 2024).
  Monthly grouping  : strftime('%Y-%m', sale_date)     → e.g. '2024-09', '2024-10', '2024-11'
  Weekly grouping   : strftime('%Y-%W', sale_date)     → ISO week number
  Hour grouping     : SUBSTR(sale_hour, 1, 2)          → '09', '14', '20'
  Consecutive months: to get the NEXT month from a given month M, use:
                      strftime('%Y-%m', M || '-01', '+1 month')
                      NEVER use plain < or > to define consecutive months — that creates non-consecutive pairs.

STANDARD DEVIATION IN SQLITE:
  SQLite has no STDDEV() function. Always compute it as:
  SQRT( AVG(x * x) - AVG(x) * AVG(x) )
  where x is the column or expression you are taking the standard deviation of.

WINDOW FUNCTIONS:
  SQLite supports: ROW_NUMBER(), RANK(), DENSE_RANK(), LAG(), LEAD(), SUM() OVER, AVG() OVER.
  Always include ORDER BY inside the OVER() clause for ranking functions.

GROWTH RATE:
  Month-over-month growth rate = (revenue_B - revenue_A) / revenue_A * 100.0
  Acceleration = the growth rate INCREASED from one period to the next (rate_2 > rate_1).
  Deceleration = the growth rate DECREASED (rate_2 < rate_1).
  This requires comparing two rates — NOT checking if a single rate is positive.

TICKET vs LINE ITEM:
  ticket_number groups all line items belonging to the same purchase.
  To count distinct tickets: COUNT(DISTINCT ticket_number).
  To count line items: COUNT(*).
"""


FEW_SHOT_EXAMPLES = """
--- EXAMPLES OF CORRECT QUERY PATTERNS ---

EXAMPLE A — Consecutive month pairs (Sep→Oct and Oct→Nov only, NOT Sep→Nov):
Question: Show revenue growth between consecutive months for each product.
Correct approach — use strftime date arithmetic to enforce strict consecutiveness:
  WITH MonthlySales AS (
    SELECT product_name,
           strftime('%Y-%m', sale_date) AS sale_month,
           SUM(total) AS revenue
    FROM sales
    WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0
    GROUP BY product_name, sale_month
  )
  SELECT ms1.product_name,
         ms1.sale_month AS month_from,
         ms2.sale_month AS month_to,
         (ms2.revenue - ms1.revenue) / ms1.revenue * 100.0 AS growth_pct
  FROM MonthlySales ms1
  JOIN MonthlySales ms2
    ON ms1.product_name = ms2.product_name
   AND ms2.sale_month = strftime('%Y-%m', ms1.sale_month || '-01', '+1 month')
Wrong: JOIN ... ON ms1.sale_month < ms2.sale_month  ← generates non-consecutive Sep→Nov pair

EXAMPLE B — Standard deviation in SQLite (no STDDEV function):
Question: Flag waiters whose revenue is more than 1 std dev above the average.
Correct:
  WITH Stats AS (
    SELECT AVG(total_rev) AS avg_rev,
           SQRT(AVG(total_rev*total_rev) - AVG(total_rev)*AVG(total_rev)) AS stddev_rev
    FROM WaiterRevenue
  )
  SELECT w.waiter, w.total_rev
  FROM WaiterRevenue w, Stats s
  WHERE w.total_rev > s.avg_rev + s.stddev_rev
Wrong: SQRT(AVG(total_rev))  ← that is SQRT of average, not standard deviation

EXAMPLE C — Acceleration means comparing two growth rates, not checking positive sign:
Question: Which products accelerated between Sep→Oct and Oct→Nov?
Correct: growth_oct_nov > growth_sep_oct  (second rate strictly higher than first rate)
Wrong:   growth_oct_nov > 0               (that is just positive growth, not acceleration)

EXAMPLE D — Top-N per group using ROW_NUMBER():
Question: Find the best-selling product for each waiter (excluding waiter 0).
Correct:
  WITH Ranked AS (
    SELECT waiter,
           product_name,
           SUM(total) AS revenue,
           ROW_NUMBER() OVER (PARTITION BY waiter ORDER BY SUM(total) DESC) AS rn
    FROM sales
    WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0
      AND waiter != 0
    GROUP BY waiter, product_name
  )
  SELECT waiter, product_name, revenue
  FROM Ranked
  WHERE rn = 1
Wrong: Using MAX(total) without PARTITION BY — gives the global top, not per-waiter top.

EXAMPLE E — Time blocks with best product and top waiter per block:
Question: For each time block (morning/midday/afternoon/evening), show total revenue, best-selling product, and top waiter excluding self-service (waiter 0).
Correct:
  WITH BlockSales AS (
    SELECT
      CASE
        WHEN SUBSTR(sale_hour, 1, 2) BETWEEN '06' AND '11' THEN 'morning'
        WHEN SUBSTR(sale_hour, 1, 2) BETWEEN '12' AND '14' THEN 'midday'
        WHEN SUBSTR(sale_hour, 1, 2) BETWEEN '15' AND '18' THEN 'afternoon'
        ELSE 'evening'
      END AS time_block,
      product_name, waiter, total
    FROM sales
    WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0
  ),
  BlockRevenue AS (
    SELECT time_block, SUM(total) AS block_revenue
    FROM BlockSales GROUP BY time_block
  ),
  BestProduct AS (
    SELECT time_block, product_name,
           ROW_NUMBER() OVER (PARTITION BY time_block ORDER BY SUM(total) DESC) AS rn
    FROM BlockSales GROUP BY time_block, product_name
  ),
  TopWaiter AS (
    SELECT time_block, waiter,
           ROW_NUMBER() OVER (PARTITION BY time_block ORDER BY SUM(total) DESC) AS rn
    FROM BlockSales WHERE waiter != 0
    GROUP BY time_block, waiter
  )
  SELECT br.time_block, br.block_revenue,
         bp.product_name AS best_product,
         tw.waiter AS top_waiter
  FROM BlockRevenue br
  LEFT JOIN BestProduct bp ON br.time_block = bp.time_block AND bp.rn = 1
  LEFT JOIN TopWaiter tw ON br.time_block = tw.time_block AND tw.rn = 1
Wrong: Using waiter_name — this column does NOT exist. The only cashier column is waiter (integer).
Wrong: WHERE sale_hour >= '12:00'  ← use SUBSTR(sale_hour, 1, 2) for hour-range comparisons.
"""


# ── Arctic-Text2SQL-R1 prompt template ───────────────────────────────────────
# Source: Appendix C of arXiv:2505.20315
# Key difference from OmniSQL: reasoning goes inside <think> tags,
# final SQL goes inside <answer> tags.

_ARCTIC_PROMPT_TEMPLATE = """Task Overview:
You are a data science expert. Below, you are provided with a database schema and a natural language question. Your task is to understand the schema and generate a valid SQL query to answer the question.

Database Engine:
SQLite

Database Schema:
{db_details}
This schema describes the database's structure, including tables, columns, primary keys, and any relevant constraints or representative values.

Question:
{question}

Instructions:
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- The generated query should return all of the information asked in the question without any missing or extra information.
- Before generating the final SQL query, think through the problem carefully and write your full reasoning process within <think> tags.
- In your <think> block, explicitly identify: (1) which columns are needed, (2) any JOIN or subquery conditions required, (3) the correct aggregation and grouping strategy, (4) which business rule flags to apply.
- Before writing your SQL, verify every column name you plan to use actually appears in the Database Schema above. Do not invent column names that are not in the schema.
- Your final answer should be enclosed within <answer> tags.
- Ensure that your SQL query follows correct SQLite syntax.

Output Format:
<think>
[Your step-by-step reasoning here]
</think>
<answer>
```sql
-- Your SQL query
```
</answer>

Take a deep breath and think step by step to find the correct SQL query."""


_ARCTIC_CORRECTION_TEMPLATE = """The following SQL query produced an error when executed against the database.

Original question: {question}

SQL that failed:
```sql
{failed_sql}
```

SQLite error message:
{error_message}

Database Schema:
{db_details}

{business_rules}

Instructions:
- Identify what caused the error from the error message.
- Think through the fix carefully within <think> tags.
- Output only the corrected SQL query inside <answer> tags.
- Do not repeat the same mistake.

<think>
[Diagnose the error and reason through the fix]
</think>
<answer>
```sql
-- Corrected SQL query
```
</answer>"""


def build_arctic_prompt(question: str) -> str:
    """Build the official Arctic-Text2SQL-R1 prompt for a natural language question."""
    enriched_question = f"{question}\n\n{BUSINESS_RULES}\n{FEW_SHOT_EXAMPLES}"
    return _ARCTIC_PROMPT_TEMPLATE.format(
        db_details=SALES_DDL,
        question=enriched_question,
    )


def build_arctic_correction_prompt(
    question: str,
    failing_sql: str,
    error_msg: str,
) -> str:
    """Build Arctic correction prompt injecting the failed SQL and error message."""
    return _ARCTIC_CORRECTION_TEMPLATE.format(
        question=question,
        failed_sql=failing_sql,
        error_message=error_msg,
        db_details=SALES_DDL,
        business_rules=BUSINESS_RULES,
    )



# ── Qwen-specific prompt system ───────────────────────────────────────────────
# qwen2.5-coder:3b is used via /api/generate with raw=True (pure completion mode,
# no chat template applied). The best format for a code-completion model is:
#   - DDL first (model sees the schema as code it should work with)
#   - Critical rules as SQL comments (-- prefix, part of the code not a document)
#   - Question as a comment immediately before the completion point
#   - Open ```sql block as the completion trigger
# No planning node, no category detection, no examples — YAGNI.

_QWEN_PROMPT_TEMPLATE = """\
{db_details}

-- Rules:
-- 1. ALWAYS filter: WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0
-- 2. Revenue = SUM(total)  |  Units sold = SUM(quantity)
-- 3. Order count = COUNT(DISTINCT ticket_number), not COUNT(*)
-- 4. Day of week: use the week_day column ('Monday', 'Friday'...). Do NOT use strftime() for this.
-- 5. Avg ticket = SUM(total) * 1.0 / COUNT(DISTINCT ticket_number)

-- {question}
```sql
"""

_QWEN_CORRECTION_TEMPLATE = """\
{db_details}

-- Question: {question}
-- Previous query (failed):
{failed_sql}
-- SQLite error: {error_message}
-- Fixed query:

```sql
"""


def build_qwen_prompt(question: str) -> str:
    """Build a code-completion-style Qwen prompt (DDL + rules as comments + trigger)."""
    return _QWEN_PROMPT_TEMPLATE.format(db_details=SALES_DDL, question=question)


def build_qwen_correction_prompt(
    question: str,
    failing_sql: str,
    error_msg: str,
) -> str:
    """Build a Qwen correction prompt embedding the failed SQL and error as comments."""
    return _QWEN_CORRECTION_TEMPLATE.format(
        db_details=SALES_DDL,
        question=question,
        failed_sql=failing_sql,
        error_message=error_msg,
    )


_ON_TOPIC_KEYWORDS = {
    "sale", "sales", "revenue", "product", "products", "waiter", "waiters",
    "ticket", "tickets", "invoice", "quantity", "price", "total",
    "week", "month", "day", "hour", "best", "top", "bottom", "most",
    "least", "cashier", "vendedor", "factura", "nota", "credito",
    "earn", "sold", "item", "never", "return", "purchase",
}


def is_on_topic(question: str) -> bool:
    """Return True if the question appears to be about sales/POS data."""
    q_lower = question.lower()
    return any(kw in q_lower for kw in _ON_TOPIC_KEYWORDS)


def extract_sql(response: str) -> str:
    """Extract the final SQL query from an Arctic-Text2SQL-R1 response.

    Priority order:
      1. Content inside <answer>...</answer> tags (official Arctic format)
      2. Last ```sql ... ``` code block anywhere in response (fallback)
      3. Last SELECT statement in response (last resort)
    """
    # Strip <think> block — never search inside it
    clean = re.sub(r"<think>.*?</think>", "", response, flags=re.DOTALL).strip()

    # Try <answer> tag extraction
    answer_matches = re.findall(r"<answer>(.*?)</answer>", clean, re.DOTALL)
    if answer_matches:
        answer_content = answer_matches[-1].strip()
        code_matches = re.findall(r"```(?:sql)?\s*(.*?)```", answer_content, re.DOTALL)
        if code_matches:
            return code_matches[-1].strip()
        return answer_content.strip()

    # Fallback — last code block anywhere in cleaned response
    code_matches = re.findall(r"```(?:sql)?\s*(.*?)```", clean, re.DOTALL | re.IGNORECASE)
    if code_matches:
        return code_matches[-1].strip()

    # Last resort — everything from last SELECT keyword
    idx = clean.rfind("SELECT")
    if idx != -1:
        return clean[idx:].strip()
    return clean.strip()


def extract_think(response: str) -> Optional[str]:
    """Extract the model's reasoning trace from <think> tags.

    Returns None if no <think> block is present (e.g. OmniSQL-style responses).
    """
    match = re.search(r"<think>(.*?)</think>", response, re.DOTALL)
    return match.group(1).strip() if match else None
