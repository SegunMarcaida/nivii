"""
SPEC-T02: Unit tests for Arctic-Text2SQL-R1 prompt system (build_arctic_prompt, extract_sql, etc.)
Zero I/O — pure string inspection.
"""
import pytest

from app.prompts.few_shot import (
    BUSINESS_RULES,
    FEW_SHOT_EXAMPLES,
    SALES_DDL,
    QueryComplexity,
    build_arctic_correction_prompt,
    build_arctic_prompt,
    build_qwen_correction_prompt,
    build_qwen_prompt,
    classify_complexity,
    extract_sql,
    extract_think,
    is_on_topic,
)


SAMPLE_QUESTION = "What is the total revenue?"


# ── DDL schema accuracy ───────────────────────────────────────────────────────

@pytest.mark.unit
def test_sales_ddl_no_waiter_name():
    """waiter_name must not be defined as a column; it may appear only in negative-guidance comments."""
    import re
    assert not re.search(r"waiter_name\s+(TEXT|INTEGER|REAL)", SALES_DDL)


@pytest.mark.unit
def test_sales_ddl_has_ticket_type():
    assert "ticket_type" in SALES_DDL


@pytest.mark.unit
def test_sales_ddl_has_is_credit_note_flag():
    assert "is_credit_note" in SALES_DDL


@pytest.mark.unit
def test_sales_ddl_has_is_promotional_flag():
    assert "is_promotional" in SALES_DDL


@pytest.mark.unit
def test_sales_ddl_has_is_manual_adj_flag():
    assert "is_manual_adj" in SALES_DDL


@pytest.mark.unit
def test_sales_ddl_documents_ticket_type_values():
    """FCB/FCA/NCB/NCA values must be documented in DDL comments."""
    assert "FCB" in SALES_DDL
    assert "NCA" in SALES_DDL


@pytest.mark.unit
def test_sales_ddl_documents_art_inexistente():
    assert "ART. INEXISTENTE" in SALES_DDL


@pytest.mark.unit
def test_sales_ddl_single_table_only():
    """The flat schema should have only one CREATE TABLE statement."""
    assert SALES_DDL.count("CREATE TABLE") == 1


@pytest.mark.unit
def test_business_rules_has_strftime():
    assert "strftime" in BUSINESS_RULES


@pytest.mark.unit
def test_business_rules_has_revenue_filter():
    assert "is_credit_note = 0" in BUSINESS_RULES


# ── Arctic DDL + BUSINESS_RULES content tests ─────────────────────────────────


class TestArcticDDLContent:
    """Verify enriched DDL contains semantically critical annotations."""

    def test_ddl_has_waiter_values(self):
        """Model needs to know valid waiter IDs to avoid hallucinating names."""
        assert "51" in SALES_DDL and "116" in SALES_DDL

    def test_ddl_has_sale_date_format(self):
        assert "YYYY-MM-DD" in SALES_DDL

    def test_ddl_has_sale_hour_format(self):
        assert "HH:MM" in SALES_DDL

    def test_ddl_has_ticket_type_values(self):
        assert "FCB" in SALES_DDL and "NCB" in SALES_DDL

    def test_ddl_no_waiter_name_column(self):
        """waiter_name must not be defined as a column (may appear in negative-guidance comments)."""
        import re
        assert not re.search(r"waiter_name\s+(TEXT|INTEGER|REAL)", SALES_DDL)

    def test_ddl_waiter_column_has_no_name_annotation(self):
        """DDL must explicitly state NO waiter_name column so the model doesn't hallucinate it."""
        assert "NO waiter_name" in SALES_DDL

    def test_ddl_waiter_has_exclude_self_service_hint(self):
        """DDL must show waiter != 0 pattern directly on the column definition."""
        assert "waiter != 0" in SALES_DDL


class TestArcticBusinessRules:
    """Verify business rules cover all known SQLite failure modes."""

    def test_has_stddev_formula(self):
        assert "SQRT" in BUSINESS_RULES and "AVG(x * x)" in BUSINESS_RULES

    def test_has_consecutive_month_formula(self):
        assert "+1 month" in BUSINESS_RULES

    def test_has_revenue_filter(self):
        assert "is_credit_note = 0" in BUSINESS_RULES

    def test_has_hour_grouping_hint(self):
        assert "SUBSTR(sale_hour, 1, 2)" in BUSINESS_RULES

    def test_has_growth_rate_definition(self):
        assert "growth" in BUSINESS_RULES.lower() and "acceleration" in BUSINESS_RULES.lower()

    def test_has_ticket_vs_line_item_distinction(self):
        assert "COUNT(DISTINCT ticket_number)" in BUSINESS_RULES

    def test_business_rules_forbids_waiter_name(self):
        """Explicit negative constraint prevents model from hallucinating waiter_name."""
        assert "waiter_name" in BUSINESS_RULES  # appears as forbidden reference

    def test_business_rules_waiter_exclude_filter(self):
        """waiter != 0 pattern must appear in business rules for self-service exclusion."""
        assert "waiter != 0" in BUSINESS_RULES



# ── Few-shot example content ──────────────────────────────────────────────────


class TestFewShotExamples:
    """Verify few-shot examples teach correct waiter and time-block patterns."""

    def test_example_e_no_waiter_name_in_correct_code(self):
        """Example E correct solution must not use the non-existent waiter_name column."""
        # Extract the 'Correct:' section of Example E
        start = FEW_SHOT_EXAMPLES.index("EXAMPLE E")
        example_e = FEW_SHOT_EXAMPLES[start:]
        correct_start = example_e.index("Correct:")
        wrong_start = example_e.index("Wrong:")
        correct_section = example_e[correct_start:wrong_start]
        assert "waiter_name" not in correct_section

    def test_example_e_uses_waiter_integer_filter(self):
        """Example E must show waiter != 0 (not a string name comparison)."""
        assert "waiter != 0" in FEW_SHOT_EXAMPLES

    def test_example_e_has_wrong_waiter_name_annotation(self):
        """Example E must call out waiter_name as wrong so the model learns the anti-pattern."""
        start = FEW_SHOT_EXAMPLES.index("EXAMPLE E")
        example_e = FEW_SHOT_EXAMPLES[start:]
        assert "waiter_name" in example_e  # appears in the Wrong: section

    def test_example_e_uses_substr_for_hour_blocks(self):
        """Example E must use SUBSTR(sale_hour, 1, 2) not full-string BETWEEN."""
        assert "SUBSTR(sale_hour, 1, 2)" in FEW_SHOT_EXAMPLES

    def test_example_e_has_row_number_for_top_waiter(self):
        """Example E must demonstrate ROW_NUMBER() for per-block top-waiter ranking."""
        start = FEW_SHOT_EXAMPLES.index("EXAMPLE E")
        example_e = FEW_SHOT_EXAMPLES[start:]
        assert "ROW_NUMBER()" in example_e


# ── extract_sql ───────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_extract_sql_from_code_block():
    response = "Some reasoning.\n```sql\nSELECT 1\n```"
    assert extract_sql(response) == "SELECT 1"


@pytest.mark.unit
def test_extract_sql_last_block():
    """OmniSQL CoT may output multiple blocks — we want the LAST one."""
    response = (
        "First attempt:\n```sql\nSELECT 1\n```\n"
        "Revised:\n```sql\nSELECT 2\n```"
    )
    assert extract_sql(response) == "SELECT 2"


@pytest.mark.unit
def test_extract_sql_plain_block_no_language():
    response = "Here is the query:\n```\nSELECT total FROM sales\n```"
    result = extract_sql(response)
    assert "SELECT total FROM sales" in result


@pytest.mark.unit
def test_extract_sql_fallback_to_select():
    """When no code block found, fall back to rfind SELECT."""
    response = "The SQL is: SELECT COUNT(*) FROM sales"
    result = extract_sql(response)
    assert result == "SELECT COUNT(*) FROM sales"


@pytest.mark.unit
def test_extract_sql_strips_whitespace():
    response = "```sql\n  SELECT 1  \n```"
    result = extract_sql(response)
    assert result == "SELECT 1"


@pytest.mark.unit
def test_extract_sql_multiline_query():
    sql = "SELECT product_name,\n  SUM(total) AS revenue\nFROM sales\nGROUP BY product_name"
    response = f"Reasoning...\n```sql\n{sql}\n```"
    result = extract_sql(response)
    assert "GROUP BY product_name" in result


@pytest.mark.unit
def test_extract_sql_with_comment_inside():
    response = "```\n-- Your SQL query\nSELECT 1\n```"
    result = extract_sql(response)
    assert "SELECT 1" in result


# ── classify_complexity ───────────────────────────────────────────────────────

class TestClassifyComplexity:
    """SIMPLE routes to Qwen; HARD routes to Arctic."""

    # --- SIMPLE: ORDER BY + LIMIT (no window function needed) ---
    def test_top_n_products_is_simple(self):
        assert classify_complexity("What are the top 5 products by revenue?") == QueryComplexity.SIMPLE

    def test_highest_revenue_month_is_simple(self):
        assert classify_complexity("Which month had the highest revenue?") == QueryComplexity.SIMPLE

    def test_most_sold_product_is_simple(self):
        assert classify_complexity("Which product sold the most units?") == QueryComplexity.SIMPLE

    def test_lowest_revenue_waiter_is_simple(self):
        assert classify_complexity("Which waiter had the lowest total sales?") == QueryComplexity.SIMPLE

    # --- SIMPLE: plain math / ratio (SUM/SUM*100, no window) ---
    def test_percentage_b2c_is_simple(self):
        assert classify_complexity("What percentage of sales were B2C?") == QueryComplexity.SIMPLE

    def test_revenue_share_product_is_simple(self):
        assert classify_complexity("What is the revenue share of alfajores?") == QueryComplexity.SIMPLE

    # --- SIMPLE: WHERE-filter exclusion (not a subquery) ---
    def test_sales_without_credit_notes_is_simple(self):
        assert classify_complexity("Show me sales without credit notes") == QueryComplexity.SIMPLE

    def test_excluding_promotions_is_simple(self):
        assert classify_complexity("Revenue excluding promotional items") == QueryComplexity.SIMPLE

    # --- HARD: monthly grouping requires strftime() ---
    def test_monthly_revenue_is_hard(self):
        """'by month' requires strftime() — routes to Arctic."""
        assert classify_complexity("Show me revenue by month") == QueryComplexity.HARD

    def test_daily_growth_no_reference_is_simple(self):
        assert classify_complexity("Show me daily sales totals") == QueryComplexity.SIMPLE

    # --- HARD: explicit window / rank ---
    def test_row_number_is_hard(self):
        assert classify_complexity("Give me a ranking of products by revenue") == QueryComplexity.HARD

    def test_dense_rank_keyword_is_hard(self):
        assert classify_complexity("Use dense_rank to rank waiters") == QueryComplexity.HARD

    def test_cumulative_revenue_is_hard(self):
        assert classify_complexity("Show cumulative revenue over the period") == QueryComplexity.HARD

    # --- HARD: Nth-order ---
    def test_second_highest_is_hard(self):
        assert classify_complexity("Which is the second highest revenue product?") == QueryComplexity.HARD

    def test_third_best_waiter_is_hard(self):
        assert classify_complexity("Who is the third best waiter?") == QueryComplexity.HARD

    # --- HARD: per-group top-N ---
    def test_top_product_per_month_is_hard(self):
        assert classify_complexity("What is the top product per month?") == QueryComplexity.HARD

    def test_best_waiter_each_week_is_hard(self):
        assert classify_complexity("Who was the best waiter each week?") == QueryComplexity.HARD

    # --- HARD: period-over-period with time reference ---
    def test_month_over_month_is_hard(self):
        assert classify_complexity("Show me month over month revenue change") == QueryComplexity.HARD

    def test_growth_vs_last_month_is_hard(self):
        assert classify_complexity("How did sales grow vs last month?") == QueryComplexity.HARD

    def test_compared_to_previous_is_hard(self):
        assert classify_complexity("Revenue compared to previous month") == QueryComplexity.HARD

    # --- HARD: true anti-join ---
    def test_never_sold_is_hard(self):
        assert classify_complexity("Which products were never sold in October?") == QueryComplexity.HARD

    def test_no_sales_period_is_hard(self):
        assert classify_complexity("Products with no sales in September") == QueryComplexity.HARD


# ── is_on_topic ───────────────────────────────────────────────────────────────

@pytest.mark.unit
def test_on_topic_sales_query():
    assert is_on_topic("What are the top 5 products by sales?") is True


@pytest.mark.unit
def test_on_topic_revenue_query():
    assert is_on_topic("What is the total revenue?") is True


@pytest.mark.unit
def test_on_topic_waiter_query():
    assert is_on_topic("Which waiter sold the most?") is True


@pytest.mark.unit
def test_off_topic_weather():
    assert is_on_topic("What is the weather in Buenos Aires?") is False


@pytest.mark.unit
def test_off_topic_unrelated():
    assert is_on_topic("Tell me a joke") is False


# ── Arctic extract_sql() tests ────────────────────────────────────────────────


class TestExtractSqlArctic:
    """Verify extract_sql handles Arctic <think>/<answer> format."""

    def test_extracts_from_answer_tag_with_code_block(self):
        response = (
            "<think>\nSome reasoning here.\n</think>\n"
            "<answer>\n```sql\nSELECT COUNT(*) FROM sales\n```\n</answer>"
        )
        result = extract_sql(response)
        assert result == "SELECT COUNT(*) FROM sales"

    def test_extracts_last_block_from_multiple_answer_blocks(self):
        """Model sometimes repeats SQL; always use the last one."""
        response = (
            "<think>reason</think>"
            "<answer>```sql\nSELECT 1\n```</answer>"
            "<answer>```sql\nSELECT 2\n```</answer>"
        )
        result = extract_sql(response)
        assert result == "SELECT 2"

    def test_fallback_to_code_block_when_no_answer_tag(self):
        """Old-style response (no <answer> tag) still works."""
        response = "Some text\n```sql\nSELECT * FROM sales\n```"
        result = extract_sql(response)
        assert result == "SELECT * FROM sales"

    def test_fallback_to_select_keyword_when_no_code_block(self):
        response = "<think>reason</think>\nSELECT id FROM sales LIMIT 1"
        result = extract_sql(response)
        assert result == "SELECT id FROM sales LIMIT 1"

    def test_think_block_not_searched_for_sql(self):
        """SQL inside <think> must be ignored; only <answer> or bare SQL counts."""
        response = (
            "<think>\n```sql\nSELECT 'wrong'\n```\n</think>\n"
            "<answer>\n```sql\nSELECT 'correct'\n```\n</answer>"
        )
        result = extract_sql(response)
        assert result == "SELECT 'correct'"

    def test_extract_think_returns_reasoning(self):
        response = "<think>\nStep 1: join tables.\n</think><answer>```sql\nSELECT 1\n```</answer>"
        think = extract_think(response)
        assert "Step 1" in think

    def test_extract_think_returns_none_when_absent(self):
        result = extract_think("No think block here.")
        assert result is None


# ── Arctic prompt build tests ─────────────────────────────────────────────────


class TestBuildArcticPrompt:
    """Verify build_arctic_prompt() produces well-formed Arctic template."""

    def test_contains_task_overview_header(self):
        prompt = build_arctic_prompt("How many sales?")
        assert "Task Overview:" in prompt

    def test_contains_database_engine_sqlite(self):
        prompt = build_arctic_prompt("How many sales?")
        assert "Database Engine:" in prompt
        assert "SQLite" in prompt

    def test_contains_database_schema_section(self):
        prompt = build_arctic_prompt("How many sales?")
        assert "Database Schema:" in prompt

    def test_contains_question(self):
        prompt = build_arctic_prompt("How many sales in October?")
        assert "How many sales in October?" in prompt

    def test_contains_think_tag_instruction(self):
        prompt = build_arctic_prompt("test")
        assert "<think>" in prompt

    def test_contains_answer_tag_instruction(self):
        prompt = build_arctic_prompt("test")
        assert "<answer>" in prompt

    def test_contains_business_rules(self):
        prompt = build_arctic_prompt("test")
        assert "BUSINESS RULES" in prompt

    def test_contains_few_shot_examples(self):
        prompt = build_arctic_prompt("test")
        assert "EXAMPLE A" in prompt and "EXAMPLE E" in prompt

    def test_contains_ddl(self):
        prompt = build_arctic_prompt("test")
        assert "CREATE TABLE sales" in prompt

    def test_contains_cot_trigger(self):
        prompt = build_arctic_prompt("test")
        assert "Take a deep breath and think step by step" in prompt


class TestBuildArcticCorrectionPrompt:
    """Verify correction prompt injects error context and uses Arctic tags."""

    def test_contains_failed_sql(self):
        prompt = build_arctic_correction_prompt(
            "How many sales?", "SELECT * FORM sales", "no such table: FORM"
        )
        assert "SELECT * FORM sales" in prompt

    def test_contains_error_message(self):
        prompt = build_arctic_correction_prompt("q", "SELECT 1", "syntax error")
        assert "syntax error" in prompt

    def test_contains_think_tag_instruction(self):
        prompt = build_arctic_correction_prompt("q", "SELECT 1", "err")
        assert "<think>" in prompt

    def test_contains_answer_tag_instruction(self):
        prompt = build_arctic_correction_prompt("q", "SELECT 1", "err")
        assert "<answer>" in prompt

    def test_contains_original_question(self):
        prompt = build_arctic_correction_prompt("Show total revenue", "SELECT 1", "err")
        assert "Show total revenue" in prompt

    def test_contains_business_rules(self):
        prompt = build_arctic_correction_prompt("q", "SELECT 1", "err")
        assert "BUSINESS RULES" in prompt


# ── Qwen prompt builder tests ─────────────────────────────────────────────────


class TestBuildQwenPrompt:
    """Verify build_qwen_prompt() produces a code-completion-style prompt."""

    def test_ddl_present(self):
        assert "CREATE TABLE sales" in build_qwen_prompt("test")

    def test_question_in_prompt(self):
        q = "How many orders were placed?"
        assert q in build_qwen_prompt(q)

    def test_ends_with_sql_trigger(self):
        assert build_qwen_prompt("test").rstrip().endswith("```sql")

    def test_no_think_tags(self):
        p = build_qwen_prompt("test")
        assert "<think>" not in p and "<answer>" not in p

    def test_revenue_filter_rule_present(self):
        assert "is_credit_note=0" in build_qwen_prompt("test")

    def test_units_rule_present(self):
        assert "SUM(quantity)" in build_qwen_prompt("test")

    def test_count_distinct_rule_present(self):
        assert "COUNT(DISTINCT ticket_number)" in build_qwen_prompt("test")

    def test_week_day_rule_present(self):
        assert "week_day" in build_qwen_prompt("test")

    def test_correction_contains_failed_sql(self):
        assert "SELCT broken" in build_qwen_correction_prompt("q", "SELCT broken", "err")

    def test_correction_contains_error(self):
        assert "near SELCT" in build_qwen_correction_prompt("q", "s", "near SELCT syntax")

    def test_correction_ends_with_sql_trigger(self):
        assert build_qwen_correction_prompt("q", "s", "e").rstrip().endswith("```sql")

    def test_correction_no_think_tags(self):
        p = build_qwen_correction_prompt("q", "s", "e")
        assert "<think>" not in p and "<answer>" not in p

    def test_correction_has_fixed_query_header(self):
        assert "-- Fixed query:" in build_qwen_correction_prompt("q", "s", "e")

    def test_no_write_sql_query_prefix(self):
        """Template-looking prefix removed to prevent document-continuation mode."""
        assert "Write a SQLite query" not in build_qwen_prompt("test")
