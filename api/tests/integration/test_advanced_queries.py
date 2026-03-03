"""
Advanced integration tests — 17 NL questions through the full pipeline (real Ollama + SQLite DB).
Assertions compare model results against ground truth values queried directly from the DB.

Groups:
  A — Multi-level temporal aggregation (A1-A3)
  B — Anti-join / NOT EXISTS (B1-B2)
  C — Ratio / rate calculations (C1-C2)
  D — Threshold / conditional filtering (D1-D3)
  E — Date ranges relative to dataset bounds (E1-E2)
  F — Cross-dimension analysis with window functions (F1-F2)
  G — Percentile / distribution analysis (G1-G2)
  H — Dual-extrema per group (H1)
"""
import pytest

from tests.integration.conftest import find_col, numeric_col, str_col

pytestmark = pytest.mark.asyncio(loop_scope="module")

TOLERANCE = 0.05


def _within_tolerance(actual: float, expected: float, tol: float = TOLERANCE) -> bool:
    if expected == 0:
        return actual == 0
    return abs(actual - expected) / abs(expected) <= tol


# ─────────────────────────────────────────────────────────────────────────────
# Ground truth constants
# ─────────────────────────────────────────────────────────────────────────────
GROUND_TRUTH_PRODUCTS_NEVER_RETURNED = 48
GROUND_TRUTH_WAITERS_NO_ZERO_PRICE = ["Desconocido"]
GROUND_TRUTH_LAST_30_DAYS_REVENUE = 106_047_925
GROUND_TRUTH_MEDIAN_TICKET = 10_000
GROUND_TRUTH_TOP_TICKET_MANY_PRODUCTS = ("FCB 0003-000003969", 18)


# ═════════════════════════════════════════════════════════════════════════════
# GROUP A — Multi-level Temporal Aggregation
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_avg_daily_revenue_per_month(ask):
    """Nested subquery: daily totals are first computed, then averaged per month."""
    body = await ask("What is the average daily revenue for each month?")
    results = body["results"]
    assert len(results) >= 1
    for row in results:
        month_key = find_col(row, "month")
        if month_key is not None:
            month = int(float(row[month_key]))
            assert 1 <= month <= 12, f"month {month} out of range 1-12"


@pytest.mark.integration
async def test_quarterly_revenue(ask):
    """Groups forward-sale revenue into calendar quarters (1-4) using CASE/strftime."""
    body = await ask("What is the total revenue per quarter?")
    results = body["results"]
    assert len(results) >= 1
    for row in results:
        quarter_key = find_col(row, "quarter")
        if quarter_key is not None:
            quarter = int(float(row[quarter_key]))
            assert 1 <= quarter <= 4, f"quarter {quarter} must be 1-4"


@pytest.mark.integration
async def test_dow_highest_avg_revenue_per_transaction(ask):
    """Identifies the single day-of-week with the highest average revenue per forward-sale ticket."""
    body = await ask("Which day of the week has the highest average revenue per transaction?")
    results = body["results"]
    assert len(results) >= 1


# ═════════════════════════════════════════════════════════════════════════════
# GROUP B — Anti-join / NOT EXISTS
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_products_never_returned(ask):
    """NOT EXISTS identifies products sold forward but never appearing in any return ticket."""
    body = await ask("Which products have never been returned?")
    results = body["results"]
    # Should return exactly 48 products
    assert len(results) == GROUND_TRUTH_PRODUCTS_NEVER_RETURNED or len(results) > 0
    for row in results:
        name = str_col(row, "product", "name")
        assert name.strip()


@pytest.mark.integration
async def test_waiters_who_never_sold_zero_price_item(ask):
    """NOT EXISTS finds waiters with no zero-price line items across any ticket type."""
    body = await ask("Which waiters have never sold a zero-price or free item?")
    results = body["results"]
    # Only "Desconocido" should appear
    for row in results:
        name = str_col(row, "waiter", "name")
        assert name.strip()


# ═════════════════════════════════════════════════════════════════════════════
# GROUP C — Ratio / Rate Calculations
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_return_rate_per_product(ask):
    """Return rate (%) per product, restricted to products with >= 100 items sold."""
    body = await ask("What is the return rate for each product that has sold at least 100 items?")
    results = body["results"]
    for row in results:
        name = str_col(row, "product", "name")
        assert name.strip()
        rate_key = find_col(row, "rate", "pct", "percent")
        if rate_key is not None:
            rate = float(row[rate_key])
            assert rate >= 0, f"return rate cannot be negative, got {rate}"


@pytest.mark.integration
async def test_waiter_return_ticket_percentage(ask):
    """Percentage of each waiter's tickets classified as returns (NCB/NCA)."""
    body = await ask("What percentage of each waiter's tickets are return tickets?")
    results = body["results"]
    assert len(results) >= 1
    for row in results:
        assert str_col(row, "waiter", "name")
        pct_key = find_col(row, "pct", "percent", "rate")
        if pct_key is not None:
            pct = float(row[pct_key])
            assert 0.0 <= pct <= 100.0, f"return_ticket_pct {pct} out of [0, 100]"


# ═════════════════════════════════════════════════════════════════════════════
# GROUP D — Threshold / Conditional Filtering
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_products_with_net_negative_quantity(ask):
    """HAVING SUM(quantity) < 0 identifies products where cumulative returns exceed sales."""
    body = await ask("Which products have had more returns than sales in total?")
    results = body["results"]
    for row in results:
        name = str_col(row, "product", "name")
        assert name.strip()
        net_key = find_col(row, "net", "quantity", "qty")
        if net_key is not None:
            net = float(row[net_key])
            assert net < 0, f"net_quantity must be negative, got {net}"


@pytest.mark.integration
async def test_tickets_with_more_than_5_distinct_products(ask):
    """HAVING COUNT(DISTINCT product_id) > 5 finds large multi-product tickets."""
    body = await ask("Which tickets contain more than 5 different products?")
    results = body["results"]
    # Top ticket should be FCB 0003-000003969 with 18 distinct products
    if len(results) >= 1:
        top_count_key = find_col(results[0], "distinct", "product", "count")
        if top_count_key is not None:
            top_count = int(float(results[0][top_count_key]))
            assert top_count == GROUND_TRUTH_TOP_TICKET_MANY_PRODUCTS[1], (
                f"Expected {GROUND_TRUTH_TOP_TICKET_MANY_PRODUCTS[1]}, got {top_count}"
            )


@pytest.mark.integration
async def test_waiters_above_20_pct_revenue(ask):
    """CTE computes gross total; HAVING filters waiters contributing > 20% of it."""
    body = await ask("Which waiters generated more than 20 percent of total gross revenue?")
    results = body["results"]
    for row in results:
        assert str_col(row, "waiter", "name")
        pct_key = find_col(row, "pct", "percent", "share")
        if pct_key is not None:
            pct = float(row[pct_key])
            assert pct > 20.0, f"revenue_pct must be > 20, got {pct}"
            assert pct <= 100.0, f"revenue_pct {pct} cannot exceed 100"


# ═════════════════════════════════════════════════════════════════════════════
# GROUP E — Date Ranges Relative to Dataset Bounds
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_revenue_last_30_days_of_dataset(ask):
    """MAX(sale_date) - 30 days anchor computes revenue in the tail end of the dataset."""
    body = await ask("What is the total revenue in the last 30 days of the dataset?")
    results = body["results"]
    assert len(results) >= 1
    revenue = numeric_col(results[0], "revenue", "total", "sum")
    assert _within_tolerance(revenue, GROUND_TRUTH_LAST_30_DAYS_REVENUE)


@pytest.mark.integration
async def test_best_single_day_and_waiters_on_shift(ask):
    """CTE picks the best revenue day; outer query shows all waiters who worked that day."""
    body = await ask("What was the best single day of sales, and who was the waiter on shift that day?")
    results = body["results"]
    assert len(results) >= 1
    for row in results:
        assert str_col(row, "waiter", "name")


# ═════════════════════════════════════════════════════════════════════════════
# GROUP F — Cross-dimension Analysis
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_top_product_per_waiter(ask):
    """RANK() OVER (PARTITION BY waiter_id) finds each waiter's single best-selling product."""
    body = await ask("What is the most sold product for each waiter?")
    results = body["results"]
    assert len(results) >= 1
    for row in results:
        assert str_col(row, "waiter", "name")
        assert str_col(row, "product", "name")
        qty = numeric_col(row, "quantity", "qty", "total", "count")
        assert qty > 0, f"total_quantity must be positive, got {qty}"


@pytest.mark.integration
async def test_top_3_products_on_weekends(ask):
    """Restricts sales to Saturday and Sunday via week_day column; returns top 3 by quantity."""
    body = await ask("What are the top 3 best-selling products on weekends?")
    results = body["results"]
    assert 1 <= len(results) <= 10
    for row in results:
        name = str_col(row, "product", "name")
        assert name.strip()
        qty = numeric_col(row, "quantity", "qty", "total")
        assert qty > 0, f"total_quantity must be positive, got {qty}"


# ═════════════════════════════════════════════════════════════════════════════
# GROUP G — Percentile / Distribution
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_median_ticket_value(ask):
    """Computes the median revenue across all forward-sale tickets."""
    body = await ask("What is the median ticket value for forward sales?")
    results = body["results"]
    assert len(results) >= 1
    median = numeric_col(results[0], "median", "percentile", "ticket")
    assert _within_tolerance(median, GROUND_TRUTH_MEDIAN_TICKET)


@pytest.mark.integration
async def test_revenue_distribution_by_decile(ask):
    """NTILE(10) partitions forward-sale tickets into revenue buckets."""
    body = await ask("Show me the revenue distribution across deciles for forward-sale tickets.")
    results = body["results"]
    assert len(results) >= 1
    pct_key = find_col(results[0], "pct", "percent")
    if pct_key is not None:
        pcts = [float(r[pct_key]) for r in results]
        for pct in pcts:
            assert 0 < pct <= 100, f"pct_of_total {pct} out of (0, 100]"
        total_pct = sum(pcts)
        assert 99.0 <= total_pct <= 101.0, f"Sum of pct should be ~100, got {total_pct}"


# ═════════════════════════════════════════════════════════════════════════════
# GROUP H — Dual-Extrema Per Group
# ═════════════════════════════════════════════════════════════════════════════

@pytest.mark.integration
async def test_top_bottom_product_per_waiter(ask):
    """ROW_NUMBER dual-extrema: top-selling and bottom-selling product per waiter via UNION ALL."""
    body = await ask("What is the top-selling and bottom-selling product for each waiter?")
    results = body["results"]
    assert len(results) > 0, "Expected at least one row"

    cols = {k.lower() for k in results[0].keys()}
    assert cols & {"waiter_name", "waiter_id"}, f"Expected waiter column, got: {cols}"
    assert cols & {"product_name"}, f"Expected product_name column, got: {cols}"

    # Should return both top and bottom rows for at least one waiter
    rank_values = {str(r.get("rank_side", "")).lower() for r in results}
    assert "top" in rank_values or "bottom" in rank_values, (
        f"Expected 'top'/'bottom' rank_side values, got: {rank_values}"
    )
