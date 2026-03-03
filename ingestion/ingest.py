"""
ingest.py — CSV data loader into a flat SQLite sales table.

Flat schema design:
- All CSV columns are preserved in a single `sales` table, nothing discarded.
- Derived columns added at load time: sale_date (ISO), ticket_type, waiter_name,
  total (pre-computed), is_credit_note, is_promotional, is_manual_adj,
  sale_month, ticket_series, product_category, product_unit.
- This lets OmniSQL-7B generate simple single-table queries without joins.

Anomaly handling:
- Negative quantities: NC* (Nota de Crédito) credit notes are valid returns. Kept.
- Fractional quantity 0.5: valid half-box sale. Stored as REAL.
- Exact duplicate rows: POS export bug on tickets with many zero-price sample lines.
  Removed via drop_duplicates() before insert. Not real double-sales.
- ART. INEXISTENTE: cashier placeholder for manual adjustments (no SKU). Kept.
- Zero-price rows: product tastings/samples. Kept (is_promotional=1 flags them).
- waiter=0: anonymous POS session (kiosk/manager). Stored as waiter_name='Desconocido'.
"""

import json
import logging
import os
import sqlite3
import sys
from datetime import datetime

import pandas as pd
import requests

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger(__name__)

SQLITE_DB_PATH   = os.environ.get("SQLITE_DB_PATH", "/app/data/confectionery.db")
OLLAMA_BASE_URL  = os.environ.get("OLLAMA_BASE_URL", "http://ollama:11434")
DATA_PATH        = "/app/data.csv"
MODEL_NAME       = os.environ.get("OLLAMA_MODEL", "a-kore/Arctic-Text2SQL-R1-7B")
MODEL_NAME_BASE  = os.environ.get("OLLAMA_MODEL_BASE", "qwen2.5-coder:3b")
ANSWER_MODEL     = os.environ.get("ANSWER_MODEL", "llama3.2:1b")
ANSWER_MODEL_BIG = os.environ.get("ANSWER_MODEL_BIG", "llama3.2:3b")

WAITER_NAMES: dict[int, str] = {
    0: "Desconocido",
}

# ─────────────────────────────────────────────────────────────────────────────
# PRODUCT MAPPINGS — 100% coverage across all 68 products / 24,212 rows
# ─────────────────────────────────────────────────────────────────────────────

PRODUCT_CATEGORY: dict[str, str] = {
    # ── Alfajor ───────────────────────────────────────────────────────────────
    "ART. INEXISTENTE":                   "Ajuste Manual",
    "Alf. 150 aniv. Suelto":             "Alfajor",
    "Alf. 150 aniv. X 8 unidades":       "Alfajor",
    "Alfajor 70 cacao caja x9un":        "Alfajor",
    "Alfajor 70 cacao x un":             "Alfajor",
    "Alfajor Sin Azucar Suelto":         "Alfajor",
    "Alfajor Sin Azucar x9 Un":          "Alfajor",
    "Alfajor Super DDL x 9 un":          "Alfajor",
    "Alfajor Super DDL x un":            "Alfajor",
    "Alfajor Vegano x 9 un":             "Alfajor",
    "Alfajor Vegano x un":               "Alfajor",
    "Alfajor choc blanco nuez x un":     "Alfajor",
    "Alfajor choc blanco x un":          "Alfajor",
    "Alfajor choc caja x12un":           "Alfajor",
    "Alfajor choc caja x6un":            "Alfajor",
    "Alfajor choc x un":                 "Alfajor",
    "Alfajor merengue fruta x un":       "Alfajor",
    "Alfajor merengue x un":             "Alfajor",
    "Alfajor mini choc blanco grane":    "Alfajor",
    "Alfajor mini choc blanco pouch":    "Alfajor",
    "Alfajor mini choc granel":          "Alfajor",
    "Alfajor mini choc pouch x125g":     "Alfajor",
    "Alfajor mini choc pouch x475g":     "Alfajor",
    "Alfajor mixto caja x12un":          "Alfajor",
    "Alfajor mixto caja x6un":           "Alfajor",
    "Alfajor semilia 70 cacao caja":     "Alfajor",
    "Alfajor semilia 70 cacao x un":     "Alfajor",
    "Alfajor surtido caja x12un":        "Alfajor",
    "Alfajor surtido caja x6un":         "Alfajor",
    # ── Barrita ───────────────────────────────────────────────────────────────
    "Barrita H con almendra":            "Barrita",
    "Barrita H con ddl":                 "Barrita",
    "Barrita cereal 70 cacao x un":      "Barrita",
    "Barrita cereal choc blanco x u":    "Barrita",
    "Barrita cereal choc leche x un":    "Barrita",
    "Barrita cereal surtida caja x1":    "Barrita",
    "Barrita cereal surtida caja x6":    "Barrita",
    # ── Conito ────────────────────────────────────────────────────────────────
    "Conito 70 cacao caja x8un":         "Conito",
    "Conito 70 cacao x un":              "Conito",
    "Conito choc blanco x un":           "Conito",
    "Conito choc caja x12un":            "Conito",
    "Conito choc caja x6un":             "Conito",
    "Conito choc x un":                  "Conito",
    "Conito coco y ddl suelto":          "Conito",
    "Conito coco y ddl x 6 un":          "Conito",
    "Conito mini choc pouch x112g":      "Conito",
    "Conito mini choc x un":             "Conito",
    "Conito mini choc x400g":            "Conito",
    "Conito mixto caja x6un":            "Conito",
    "Conito mixto choc caja x12un":      "Conito",
    # ── Coronita ──────────────────────────────────────────────────────────────
    "Coronita 70% x 10 unidades":        "Coronita",
    "Coronita surtidas x 20 unidade":    "Coronita",
    "Coronitas choc tripack":            "Coronita",
    # ── Galletita (includes Medallon) ─────────────────────────────────────────
    "Galletita choc limon caja x12u":    "Galletita",
    "Galletita choc limon x un":         "Galletita",
    "Galletita limon caja x12un":        "Galletita",
    "Galletita limon x un":              "Galletita",
    "Galletita mini choc limon pouc":    "Galletita",
    "Medallon choc limon pouch x200":    "Galletita",
    # ── Tableta ───────────────────────────────────────────────────────────────
    "Tableta 70 cacao x80g":             "Tableta",
    "Tableta blanco x80g":               "Tableta",
    "Tableta cacao al 85%":              "Tableta",
    "Tableta choco 70% con almendra":    "Tableta",
    # ── Trufa ─────────────────────────────────────────────────────────────────
    "Trufas 70% cacao lata x 200g":      "Trufa",
    # ── MIX / Other ───────────────────────────────────────────────────────────
    "Caja MIX Locales":                  "MIX",
    "Miniaturas x 48 unidades":          "MIX",
    "Pouch x 475 grs. Mini Surtidos":    "MIX",
    # ── Dulce de Leche ────────────────────────────────────────────────────────
    "Dulce de leche vidrio x450g":       "Dulce de Leche",
    "Dulce de leche vidrio x800g":       "Dulce de Leche",
}

PRODUCT_UNIT: dict[str, str] = {
    # ── No packaging ──────────────────────────────────────────────────────────
    "ART. INEXISTENTE":                   "N/A",
    # ── Individual units (1u) ─────────────────────────────────────────────────
    "Alf. 150 aniv. Suelto":             "1u",
    "Alfajor 70 cacao x un":             "1u",
    "Alfajor Sin Azucar Suelto":         "1u",
    "Alfajor Super DDL x un":            "1u",
    "Alfajor Vegano x un":               "1u",
    "Alfajor choc blanco nuez x un":     "1u",
    "Alfajor choc blanco x un":          "1u",
    "Alfajor choc x un":                 "1u",
    "Alfajor merengue fruta x un":       "1u",
    "Alfajor merengue x un":             "1u",
    "Alfajor semilia 70 cacao x un":     "1u",
    "Barrita H con almendra":            "1u",
    "Barrita H con ddl":                 "1u",
    "Barrita cereal 70 cacao x un":      "1u",
    "Barrita cereal choc blanco x u":    "1u",
    "Barrita cereal choc leche x un":    "1u",
    "Conito 70 cacao x un":              "1u",
    "Conito choc blanco x un":           "1u",
    "Conito choc x un":                  "1u",
    "Conito coco y ddl suelto":          "1u",
    "Conito mini choc x un":             "1u",
    "Galletita choc limon x un":         "1u",
    "Galletita limon x un":              "1u",
    # ── Boxes: 3u ─────────────────────────────────────────────────────────────
    "Coronitas choc tripack":            "3u",
    # ── Boxes: 6u ─────────────────────────────────────────────────────────────
    "Alfajor choc caja x6un":            "6u",
    "Alfajor mixto caja x6un":           "6u",
    "Alfajor surtido caja x6un":         "6u",
    "Barrita cereal surtida caja x6":    "6u",
    "Conito choc caja x6un":             "6u",
    "Conito coco y ddl x 6 un":          "6u",
    "Conito mixto caja x6un":            "6u",
    # ── Boxes: 8u ─────────────────────────────────────────────────────────────
    "Alf. 150 aniv. X 8 unidades":       "8u",
    "Conito 70 cacao caja x8un":         "8u",
    # ── Boxes: 9u ─────────────────────────────────────────────────────────────
    "Alfajor 70 cacao caja x9un":        "9u",
    "Alfajor Sin Azucar x9 Un":          "9u",
    "Alfajor Super DDL x 9 un":          "9u",
    "Alfajor Vegano x 9 un":             "9u",
    "Alfajor semilia 70 cacao caja":     "9u",
    # ── Boxes: 10u ────────────────────────────────────────────────────────────
    "Coronita 70% x 10 unidades":        "10u",
    # ── Boxes: 12u ────────────────────────────────────────────────────────────
    "Alfajor choc caja x12un":           "12u",
    "Alfajor mixto caja x12un":          "12u",
    "Alfajor surtido caja x12un":        "12u",
    "Barrita cereal surtida caja x1":    "12u",
    "Caja MIX Locales":                  "12u",
    "Conito choc caja x12un":            "12u",
    "Conito mixto choc caja x12un":      "12u",
    "Galletita choc limon caja x12u":    "12u",
    "Galletita limon caja x12un":        "12u",
    # ── Boxes: 20u ────────────────────────────────────────────────────────────
    "Coronita surtidas x 20 unidade":    "20u",
    # ── Boxes: 48u ────────────────────────────────────────────────────────────
    "Miniaturas x 48 unidades":          "48u",
    # ── Weight: 80g ───────────────────────────────────────────────────────────
    "Tableta 70 cacao x80g":             "80g",
    "Tableta blanco x80g":               "80g",
    "Tableta cacao al 85%":              "80g",
    "Tableta choco 70% con almendra":    "80g",
    # ── Weight: 112g ──────────────────────────────────────────────────────────
    "Conito mini choc pouch x112g":      "112g",
    # ── Weight: 125g ──────────────────────────────────────────────────────────
    "Alfajor mini choc pouch x125g":     "125g",
    # ── Weight: 200g ──────────────────────────────────────────────────────────
    "Medallon choc limon pouch x200":    "200g",
    "Trufas 70% cacao lata x 200g":      "200g",
    # ── Weight: 400g ──────────────────────────────────────────────────────────
    "Conito mini choc x400g":            "400g",
    # ── Weight: 450g ──────────────────────────────────────────────────────────
    "Dulce de leche vidrio x450g":       "450g",
    # ── Weight: 475g ──────────────────────────────────────────────────────────
    "Alfajor mini choc blanco grane":    "475g",
    "Alfajor mini choc blanco pouch":    "475g",
    "Alfajor mini choc granel":          "475g",
    "Alfajor mini choc pouch x475g":     "475g",
    "Galletita mini choc limon pouc":    "475g",
    "Pouch x 475 grs. Mini Surtidos":    "475g",
    # ── Weight: 800g ──────────────────────────────────────────────────────────
    "Dulce de leche vidrio x800g":       "800g",
}

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS sales (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    sale_date        TEXT NOT NULL,
    week_day         TEXT NOT NULL,
    sale_hour        TEXT NOT NULL,
    ticket_number    TEXT NOT NULL,
    ticket_type      TEXT NOT NULL,
    waiter           INTEGER,
    waiter_name      TEXT,
    product_name     TEXT NOT NULL,
    quantity         REAL NOT NULL,
    unitary_price    REAL NOT NULL,
    total            REAL NOT NULL,
    is_credit_note   INTEGER NOT NULL DEFAULT 0,
    is_promotional   INTEGER NOT NULL DEFAULT 0,
    is_manual_adj    INTEGER NOT NULL DEFAULT 0,
    sale_month       TEXT NOT NULL,
    ticket_series    INTEGER NOT NULL,
    product_category TEXT NOT NULL,
    product_unit     TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_sales_date     ON sales(sale_date);
CREATE INDEX IF NOT EXISTS idx_sales_waiter   ON sales(waiter);
CREATE INDEX IF NOT EXISTS idx_sales_product  ON sales(product_name);
CREATE INDEX IF NOT EXISTS idx_sales_ticket   ON sales(ticket_number);
CREATE INDEX IF NOT EXISTS idx_sale_month     ON sales(sale_month);
CREATE INDEX IF NOT EXISTS idx_ticket_series  ON sales(ticket_series);
CREATE INDEX IF NOT EXISTS idx_product_cat    ON sales(product_category);
CREATE INDEX IF NOT EXISTS idx_product_unit   ON sales(product_unit);
"""


def _waiter_name(waiter_id: int) -> str:
    return WAITER_NAMES.get(waiter_id, f"Vendedor {waiter_id}")


def get_connection() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(SQLITE_DB_PATH), exist_ok=True)
    conn = sqlite3.connect(SQLITE_DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def is_already_loaded(conn: sqlite3.Connection) -> bool:
    try:
        count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
        if count > 0:
            log.info("sales already has %d rows — skipping ingestion.", count)
            return True
        return False
    except sqlite3.OperationalError:
        return False


def create_schema(conn: sqlite3.Connection) -> None:
    conn.executescript(CREATE_TABLE_SQL)
    conn.commit()
    log.info("Schema created.")


def load_data(conn: sqlite3.Connection, df: pd.DataFrame) -> None:
    log.info("Starting data load into flat sales table...")

    # ── Derived columns ──────────────────────────────────────────────────────
    # Convert MM/DD/YYYY → YYYY-MM-DD for SQLite date functions
    df["sale_date"] = df["date"].apply(
        lambda d: datetime.strptime(d.strip(), "%m/%d/%Y").strftime("%Y-%m-%d")
    )
    # Rename hour → sale_hour (avoid SQLite reserved word collision)
    df = df.rename(columns={"hour": "sale_hour"})

    # ticket_type from prefix of ticket_number (FCB/FCA/NCB/NCA)
    df["ticket_type"] = df["ticket_number"].str.split().str[0]

    # Waiter display name
    df["waiter_name"] = df["waiter"].apply(_waiter_name)

    # Pre-compute total (quantity * unitary_price)
    df["total"] = df["quantity"] * df["unitary_price"]

    # Pre-computed boolean flags as integers (0/1)
    df["is_credit_note"] = df["ticket_type"].isin(["NCB", "NCA"]).astype(int)
    df["is_promotional"] = (df["unitary_price"] == 0.0).astype(int)
    df["is_manual_adj"]  = (df["product_name"] == "ART. INEXISTENTE").astype(int)

    # ── Enriched derived columns ─────────────────────────────────────────────
    # sale_month: 'YYYY-MM' extracted from ISO sale_date
    df["sale_month"] = df["sale_date"].str[:7]

    # ticket_series: POS terminal ID from ticket_number prefix
    # 'FCB 0003-000024735' → 3,  'FCA 0001-000000043' → 1
    df["ticket_series"] = (
        df["ticket_number"]
        .str.extract(r"^\w+\s+0*(\d+)-\d+$")[0]
        .astype(int)
    )

    # product_category / product_unit: mapped from product_name
    unknown_products = set(df["product_name"].unique()) - set(PRODUCT_CATEGORY)
    if unknown_products:
        raise ValueError(
            f"Unmapped products — update PRODUCT_CATEGORY/PRODUCT_UNIT: {unknown_products}"
        )
    df["product_category"] = df["product_name"].map(PRODUCT_CATEGORY)
    df["product_unit"]     = df["product_name"].map(PRODUCT_UNIT)

    # ── Select final columns in table order ─────────────────────────────────
    columns = [
        "sale_date", "week_day", "sale_hour",
        "ticket_number", "ticket_type",
        "waiter", "waiter_name",
        "product_name", "quantity", "unitary_price", "total",
        "is_credit_note", "is_promotional", "is_manual_adj",
        "sale_month", "ticket_series", "product_category", "product_unit",
    ]
    rows = df[columns].values.tolist()

    placeholders = ", ".join(["?"] * len(columns))
    col_names = ", ".join(columns)
    insert_sql = f"INSERT INTO sales ({col_names}) VALUES ({placeholders})"

    log.info("Inserting %d rows...", len(rows))
    chunk_size = 1000
    with conn:
        for i in range(0, len(rows), chunk_size):
            conn.executemany(insert_sql, rows[i : i + chunk_size])

    final_count = conn.execute("SELECT COUNT(*) FROM sales").fetchone()[0]
    log.info("Load complete. Total rows in sales: %d", final_count)


def model_exists(model_name: str) -> bool:
    try:
        response = requests.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=10)
        response.raise_for_status()
        names = [m.get("name") for m in response.json().get("models", [])]
        return model_name in names
    except requests.exceptions.RequestException:
        return False


def pull_model(model_name: str) -> None:
    log.info("Pulling Ollama model '%s'... (this may take several minutes)", model_name)
    try:
        with requests.post(
            f"{OLLAMA_BASE_URL}/api/pull",
            json={"name": model_name, "stream": True},
            stream=True,
            timeout=(30, None),
        ) as response:
            response.raise_for_status()
            last_status = ""
            for line in response.iter_lines():
                if line:
                    try:
                        data = json.loads(line)
                        status = data.get("status", "")
                        if status != last_status:
                            log.info("Model pull: %s", status)
                            last_status = status
                    except json.JSONDecodeError:
                        pass
        log.info("Model '%s' ready.", model_name)
    except requests.exceptions.ConnectionError as exc:
        log.error("Model pull failed — Ollama not reachable: %s", exc)
        raise
    except requests.exceptions.RequestException as exc:
        log.error("Model pull failed: %s", exc)
        raise


def main() -> None:
    log.info("Ingestion service starting...")

    conn = get_connection()
    try:
        if is_already_loaded(conn):
            log.info("Data already loaded. Skipping.")
        else:
            create_schema(conn)

            log.info("Reading CSV from %s...", DATA_PATH)
            df = pd.read_csv(
                DATA_PATH,
                dtype={
                    "ticket_number": str,
                    "date":          str,
                    "hour":          str,
                    "waiter":        int,
                    "product_name":  str,
                    "quantity":      float,
                    "unitary_price": float,
                },
            )
            raw_count = len(df)
            log.info("CSV loaded: %d rows, columns: %s", raw_count, list(df.columns))

            # ── Deduplication (POS export bug) ────────────────────────────────
            df = df.drop_duplicates()
            dedup_count = len(df)
            log.info(
                "After deduplication: %d rows (discarded %d exact duplicates)",
                dedup_count, raw_count - dedup_count,
            )

            # ── Data quality summary ──────────────────────────────────────────
            ticket_types_preview = df["ticket_number"].str.split().str[0].value_counts().to_dict()
            log.info("=== Data Quality Summary ===")
            log.info("  Total rows (after dedup): %d", dedup_count)
            log.info("  Unique tickets:           %d", df["ticket_number"].nunique())
            log.info("  Unique products:          %d", df["product_name"].nunique())
            log.info("  Unique waiters:           %d", df["waiter"].nunique())
            log.info("  Ticket types:             %s", ticket_types_preview)
            log.info("  Negative qty rows:        %d", int((df["quantity"] < 0).sum()))
            log.info("  Zero-price rows:          %d", int((df["unitary_price"] == 0).sum()))
            log.info("============================")

            load_data(conn, df)
    finally:
        conn.close()

    for mname in (MODEL_NAME, MODEL_NAME_BASE, ANSWER_MODEL, ANSWER_MODEL_BIG):
        if model_exists(mname):
            log.info("Model '%s' already available — skipping pull.", mname)
        else:
            pull_model(mname)

    log.info("Ingestion complete. Exiting.")
    sys.exit(0)


if __name__ == "__main__":
    main()
