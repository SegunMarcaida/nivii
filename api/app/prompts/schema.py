"""
Database schema definition — the enriched DDL for the flat sales table.

Column comments include representative values, format examples, and semantic
descriptions that guide the LLM's SQL generation.
"""

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
    is_manual_adj       INTEGER NOT NULL DEFAULT 0,  -- 1 if product_name = 'ART. INEXISTENTE', else 0

    -- DERIVED / PRE-COMPUTED ENRICHMENT COLUMNS
    product_category    TEXT NOT NULL,   -- Product type. Values: 'Alfajor','Barrita','Conito','Coronita','Galletita','Tableta','Trufa','MIX','Dulce de Leche','Ajuste Manual'. No products table exists.
    product_unit        TEXT NOT NULL,   -- Packaging size. Values: '1u','3u','6u','8u','9u','10u','12u','20u','48u','80g','112g','125g','200g','400g','450g','475g','800g','N/A'.
    sale_month          TEXT NOT NULL,   -- Pre-computed YYYY-MM from sale_date. Examples: '2024-09','2024-10','2024-11'. Use instead of strftime('%Y-%m', sale_date).
    ticket_series       INTEGER NOT NULL -- POS terminal ID from ticket_number prefix. Examples: 1, 3.
);
"""
