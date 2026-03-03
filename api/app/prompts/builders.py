"""
Prompt builders for Arctic and Qwen models.

Each function composes a complete prompt from schema (SALES_DDL),
business rules, and few-shot examples using the appropriate template
format for the target model.
"""

from app.prompts.schema import SALES_DDL
from app.prompts.templates import render_arctic_prompt, _qwen_rules_block


def build_arctic_prompt(question: str) -> str:
    """Build the official Arctic-Text2SQL-R1 prompt for a natural language question."""
    return render_arctic_prompt(question)


def build_arctic_correction_prompt(
    question: str,
    failing_sql: str,
    error_msg: str,
) -> str:
    """Build Arctic correction prompt using the official Arctic template format.

    Embeds the failed SQL and error message in the Question field alongside
    business rules.  FEW_SHOT_EXAMPLES are excluded to fit within the 4096
    token context window (the error context already consumes extra tokens).
    """
    return render_arctic_prompt(question, failed_sql=failing_sql, error_msg=error_msg)


# ── Qwen-specific prompt system ───────────────────────────────────────────────
# qwen2.5-coder:3b is used via /api/generate with raw=True (pure completion mode,
# no chat template applied). The best format for a code-completion model is:
#   - DDL first (model sees the schema as code it should work with)
#   - Critical rules as SQL comments (-- prefix, part of the code not a document)
#   - Question as a comment immediately before the completion point
#   - Open ```sql block as the completion trigger

# ── Planning prompt for Qwen ──────────────────────────────────────────────────
# Uses distinct "planning rules" (not the standard 6) to elicit column/formula
# identification BEFORE SQL generation. Stop sequences prevent SQL drift.
_QWEN_PLAN_TEMPLATE = """\
{db_details}

-- Planning rules:
-- 1. Revenue filter: is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0
-- 2. Avg ticket = SUM(total) * 1.0 / COUNT(DISTINCT ticket_number)
-- 3. Hour grouping: SUBSTR(sale_hour, 1, 2)
-- 4. waiter column is INTEGER id; waiter=0 is self-service
-- 5. Standard deviation: SQRT(AVG(x*x) - AVG(x)*AVG(x))
-- 6. product_category groups products (Alfajor, Barrita, Conito, etc.) — ONE table only, no JOINs

-- Question: {question}
-- Columns needed:"""


def build_qwen_plan_prompt(question: str) -> str:
    """Build a planning prompt for Qwen that identifies relevant columns and operations.

    Output is SQL comments only (code-model friendly format). The caller re-attaches
    the '-- Columns needed:' prefix that is cut off by the stop sequence.
    """
    return _QWEN_PLAN_TEMPLATE.format(db_details=SALES_DDL, question=question)


def build_qwen_prompt(question: str, plan: str = "") -> str:
    """Build a code-completion-style Qwen prompt (DDL + rules as comments + trigger).

    If *plan* is provided (output from build_qwen_plan_prompt), it is injected as
    a SQL comment block between the question and the ```sql trigger so the model
    generates SQL anchored to the pre-identified columns and formula.
    """
    plan_block = f"\n{plan.strip()}" if plan.strip() else ""
    rules = _qwen_rules_block()
    return f"{rules}\n\n-- {question}{plan_block}\n```sql\n"


def build_qwen_correction_prompt(
    question: str,
    failing_sql: str,
    error_msg: str,
) -> str:
    """Build a Qwen correction prompt embedding the failed SQL and error as comments."""
    rules = _qwen_rules_block()
    return (
        f"{rules}\n\n"
        f"-- Question: {question}\n"
        f"-- Previous query (failed):\n"
        f"{failing_sql}\n"
        f"-- SQLite error: {error_msg}\n"
        f"-- Fixed query:\n\n"
        "```sql\n"
    )
