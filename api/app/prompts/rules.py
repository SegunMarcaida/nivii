"""
Business rules and few-shot examples for the NL2SQL prompt system.

BUSINESS_RULES are appended to every prompt (Arctic and Qwen) to ground the
model's SQL generation. FEW_SHOT_EXAMPLES are appended to fresh Arctic prompts
only (correction prompts omit them to save context window tokens).
"""

BUSINESS_RULES = """
--- BUSINESS RULES (apply to every query unless explicitly asked otherwise) ---

SCHEMA CONSTRAINT — ONE TABLE ONLY:
  The database has exactly one table: sales. Never JOIN or reference any other table.
  All product, category, packaging, waiter, and date information exists as columns in sales.
  product_category is a column in sales — do NOT invent a products, categories, or items table.

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
  Average ticket value = SUM(total) * 1.0 / COUNT(DISTINCT ticket_number).

AVERAGE PER-DAY METRICS:
  "Average daily X per month" = first SUM(X) GROUP BY sale_date (daily totals),
  then AVG(daily_total) GROUP BY strftime('%Y-%m', sale_date) (monthly averages).
  Always use a subquery — AVG(X) directly gives per-row average, not per-day average.
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

EXAMPLE F — Average ticket value:
Question: What is the average ticket value?
Correct:
  SELECT SUM(total) * 1.0 / COUNT(DISTINCT ticket_number) AS avg_ticket_value
  FROM sales
  WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0;

EXAMPLE G — Average daily revenue per month (subquery required):
Question: What is the average daily revenue for each month?
Correct:
  SELECT month, AVG(daily_revenue) AS avg_daily_revenue
  FROM (
    SELECT strftime('%Y-%m', sale_date) AS month,
           sale_date,
           SUM(total) AS daily_revenue
    FROM sales
    WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0
    GROUP BY sale_date
  ) daily
  GROUP BY month;
Wrong: AVG(total) GROUP BY month  ← per-row average, not per-day average.
Wrong: Referencing sale_date in outer query when only month and daily_revenue are in subquery SELECT.

EXAMPLE H — Category revenue (product_category is a column in sales, not a separate table):
Question: Which product category brings the most revenue?
Correct:
  SELECT product_category, SUM(total) AS revenue
  FROM sales
  WHERE is_credit_note=0 AND is_promotional=0 AND is_manual_adj=0
  GROUP BY product_category
  ORDER BY revenue DESC
  LIMIT 1;
Wrong: JOIN to a products or categories table — product_category is a column in sales. There is no products table.
Wrong: WHERE category_name = ... — no category_name column; use product_category.
"""
