"""
SPEC-T01: Unit tests for extract_sql() from the OmniSQL response parser.
Zero I/O — pure string transformation.
"""
import pytest

from app.prompts.parsers import extract_sql


@pytest.mark.unit
def test_extracts_sql_from_fenced_block():
    raw = "```sql\nSELECT 1\n```"
    assert extract_sql(raw) == "SELECT 1"


@pytest.mark.unit
def test_extracts_last_code_block():
    """OmniSQL outputs CoT before the final SQL — we always want the LAST block."""
    raw = "```sql\nSELECT 1\n```\nAfter review:\n```sql\nSELECT 2\n```"
    assert extract_sql(raw) == "SELECT 2"


@pytest.mark.unit
def test_extracts_plain_fence_no_language():
    raw = "```\nSELECT total FROM sales\n```"
    result = extract_sql(raw)
    assert "SELECT total FROM sales" in result


@pytest.mark.unit
def test_strips_leading_trailing_whitespace():
    raw = "```sql\n  SELECT 1  \n```"
    assert extract_sql(raw) == "SELECT 1"


@pytest.mark.unit
def test_passthrough_when_no_fences():
    """If response has no code block, return everything from the last SELECT."""
    raw = "SELECT 1"
    assert extract_sql(raw) == "SELECT 1"


@pytest.mark.unit
def test_fallback_to_last_select():
    raw = "Let me think... The answer is: SELECT COUNT(*) FROM sales"
    result = extract_sql(raw)
    assert result == "SELECT COUNT(*) FROM sales"


@pytest.mark.unit
def test_multiline_sql_preserved():
    raw = "```sql\nSELECT\n  a, b\nFROM sales\n```"
    result = extract_sql(raw)
    assert "SELECT" in result
    assert "a, b" in result
    assert "FROM sales" in result
    assert "```" not in result


@pytest.mark.unit
def test_case_insensitive_fence_language():
    raw = "```SQL\nSELECT 1\n```"
    assert extract_sql(raw) == "SELECT 1"


@pytest.mark.unit
def test_real_omnisql_cot_response():
    """Simulate the typical OmniSQL output: reasoning text + final code block."""
    raw = (
        "Let me analyze the question step by step.\n"
        "- We need total revenue\n"
        "- Filter credit notes and promotions\n"
        "- Use SUM(total)\n\n"
        "```sql\n"
        "SELECT SUM(total) AS total_revenue\n"
        "FROM sales\n"
        "WHERE is_credit_note = 0\n"
        "  AND is_promotional = 0\n"
        "  AND is_manual_adj = 0\n"
        "```"
    )
    result = extract_sql(raw)
    assert "SELECT SUM(total)" in result
    assert "WHERE is_credit_note = 0" in result
    assert "```" not in result


@pytest.mark.unit
def test_extracts_cte_query():
    raw = (
        "```sql\n"
        "WITH ranked AS (\n"
        "  SELECT product_name, SUM(total) AS revenue,\n"
        "    ROW_NUMBER() OVER (ORDER BY SUM(total) DESC) AS rn\n"
        "  FROM sales\n"
        "  WHERE is_credit_note = 0\n"
        "  GROUP BY product_name\n"
        ")\n"
        "SELECT product_name, revenue FROM ranked WHERE rn <= 5\n"
        "```"
    )
    result = extract_sql(raw)
    assert "WITH ranked AS" in result
    assert "ROW_NUMBER()" in result


@pytest.mark.unit
def test_comment_inside_code_block_is_included():
    """SQL comments inside code blocks should be preserved."""
    raw = "```\n-- Revenue query\nSELECT SUM(total) FROM sales\n```"
    result = extract_sql(raw)
    assert "SELECT SUM(total)" in result
