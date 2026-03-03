"""
Composable Arctic prompt sections — eliminates template duplication in builders.py.

The two Arctic variants (fresh and correction) share the same header and output
format blocks. This module exposes render_arctic_prompt() as the single entry
point that composes the right variant-specific instructions.

Also exports _qwen_rules_block() for shared DDL+rules used across all 3 Qwen templates.
"""

from app.prompts.rules import BUSINESS_RULES, FEW_SHOT_EXAMPLES
from app.prompts.schema import SALES_DDL


# ── Shared Arctic sections ────────────────────────────────────────────────────

_ARCTIC_TASK_HEADER = (
    "Task Overview:\n"
    "You are a data science expert. Below, you are provided with a database schema "
    "and a natural language question. Your task is to understand the schema and "
    "generate a valid SQL query to answer the question."
)

_ARCTIC_CORRECTION_TASK_HEADER = (
    "Task Overview:\n"
    "You are a data science expert. A previous SQL query attempt failed with an error. "
    "Your task is to diagnose the error and generate a corrected SQL query."
)

_ARCTIC_ENGINE_SECTION = "Database Engine:\nSQLite"

_ARCTIC_SCHEMA_SECTION = (
    "Database Schema:\n"
    "{db_details}"
    "This schema describes the database's structure, including tables, columns, "
    "primary keys, and any relevant constraints or representative values."
)

_ARCTIC_INSTRUCTIONS_FRESH = """\
Instructions:
- Make sure you only output the information that is asked in the question. If the question asks for a specific column, make sure to only include that column in the SELECT clause, nothing more.
- The generated query should return all of the information asked in the question without any missing or extra information.
- Before generating the final SQL query, think through the problem carefully and write your full reasoning process within <think> tags.
- In your <think> block, explicitly identify: (1) which columns are needed, (2) any JOIN or subquery conditions required, (3) the correct aggregation and grouping strategy, (4) which business rule flags to apply.
- Before writing your SQL, verify every column name you plan to use actually appears in the Database Schema above. Do not invent column names that are not in the schema.
- Your final answer should be enclosed within <answer> tags.
- Ensure that your SQL query follows correct SQLite syntax."""

_ARCTIC_INSTRUCTIONS_CORRECTION = """\
Instructions:
- Carefully read the error message and identify what caused the failure.
- If the error is about a missing column, verify every column name in the outer query actually appears in the subquery's SELECT list. Subquery aliases (e.g. sale_day) are NOT the same as original table columns (e.g. sale_date).
- Before generating the corrected SQL, think through the diagnosis and fix within <think> tags.
- Your corrected SQL must be enclosed within <answer> tags.
- Ensure correct SQLite syntax."""

_ARCTIC_OUTPUT_FORMAT_FRESH = """\
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

_ARCTIC_OUTPUT_FORMAT_CORRECTION = """\
Output Format:
<think>
[Diagnose the error and reason through the fix]
</think>
<answer>
```sql
-- Corrected SQL query
```
</answer>

Take a deep breath and carefully fix the query."""


def render_arctic_prompt(
    question: str,
    failed_sql: str | None = None,
    error_msg: str | None = None,
) -> str:
    """Render a complete Arctic prompt for either fresh generation or error correction.

    Args:
        question:   The natural language question to answer.
        failed_sql: If provided, renders the correction variant with this failed SQL.
        error_msg:  Required when failed_sql is provided; the error from EXPLAIN.

    Returns the fully composed prompt string.
    """
    is_correction = failed_sql is not None

    task_header = _ARCTIC_CORRECTION_TASK_HEADER if is_correction else _ARCTIC_TASK_HEADER
    schema_section = _ARCTIC_SCHEMA_SECTION.format(db_details=SALES_DDL)
    instructions = _ARCTIC_INSTRUCTIONS_CORRECTION if is_correction else _ARCTIC_INSTRUCTIONS_FRESH
    output_format = _ARCTIC_OUTPUT_FORMAT_CORRECTION if is_correction else _ARCTIC_OUTPUT_FORMAT_FRESH

    # Enrich the question field with business rules (and few-shots for fresh only)
    if is_correction:
        enriched_question = f"{question}\n\n{BUSINESS_RULES}"
    else:
        enriched_question = f"{question}\n\n{BUSINESS_RULES}\n{FEW_SHOT_EXAMPLES}"

    question_section = f"Question:\n{enriched_question}"

    if is_correction:
        failed_block = f"Previous SQL attempt that FAILED:\n```sql\n{failed_sql}\n```\nError: {error_msg}\n\nFix the SQL above. Do NOT repeat the same mistake."
        sections = [
            task_header,
            _ARCTIC_ENGINE_SECTION,
            schema_section,
            question_section,
            failed_block,
            instructions,
            output_format,
        ]
    else:
        sections = [
            task_header,
            _ARCTIC_ENGINE_SECTION,
            schema_section,
            question_section,
            instructions,
            output_format,
        ]

    return "\n\n".join(sections)


# ── Shared Qwen rules block ───────────────────────────────────────────────────

def _qwen_rules_block() -> str:
    """Return the shared DDL + SQL-comment rules used in all three Qwen templates."""
    return (
        f"{SALES_DDL}\n\n"
        "-- Rules:\n"
        "-- 1. ALWAYS filter: WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0\n"
        "-- 2. Revenue = SUM(total)  |  Units sold = SUM(quantity)\n"
        "-- 3. Order count = COUNT(DISTINCT ticket_number), not COUNT(*)\n"
        "-- 4. Day of week: use the week_day column ('Monday', 'Friday'...). Do NOT use strftime() for this.\n"
        "-- 5. Avg ticket = SUM(total) * 1.0 / COUNT(DISTINCT ticket_number)\n"
        "-- 6. ONE table only (sales). product_category = 'Alfajor','Barrita','Conito','Coronita','Galletita','Tableta','Trufa','MIX','Dulce de Leche','Ajuste Manual'. Never JOIN."
    )
