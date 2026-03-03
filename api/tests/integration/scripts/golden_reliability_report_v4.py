"""Run golden reliability evaluation in non-blocking report mode by default."""
from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path

import httpx
from httpx import ASGITransport, AsyncClient

os.environ.setdefault("OLLAMA_BASE_URL", "http://localhost:11434")

from app.main import app  # noqa: E402


def _load_queries(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(data, list):
        raise ValueError("Golden fixture must be a list")
    return data


def _is_sql_shape_valid(sql: str) -> bool:
    if not sql:
        return False
    sql_l = sql.strip().lower()
    return sql_l.startswith("select") or sql_l.startswith("with")


async def _run_eval(queries: list[dict], limit: int | None = None) -> dict:
    selected = queries[:limit] if limit else queries
    timeout = httpx.Timeout(connect=60.0, read=600.0, write=60.0, pool=30.0)
    transport = ASGITransport(app=app)

    total = len(selected)
    valid = 0
    failures: list[dict] = []

    async with AsyncClient(transport=transport, base_url="http://test", timeout=timeout) as client:
        for i, item in enumerate(selected, 1):
            q = item["question"]
            try:
                response = await client.post("/query", json={"question": q})
                if response.status_code != 200:
                    failures.append({"index": i, "question": q, "error": f"status={response.status_code}"})
                    continue
                body = response.json()
                row_count_ok = body.get("row_count") == len(body.get("results", []))
                sql_ok = _is_sql_shape_valid(body.get("sql", ""))
                if row_count_ok and sql_ok:
                    valid += 1
                else:
                    failures.append(
                        {
                            "index": i,
                            "question": q,
                            "error": f"row_count_ok={row_count_ok}, sql_ok={sql_ok}",
                        }
                    )
            except Exception as exc:  # noqa: BLE001
                failures.append({"index": i, "question": q, "error": f"{type(exc).__name__}: {exc}"})

    return {
        "total": total,
        "valid": valid,
        "valid_rate": (valid / total) if total else 0.0,
        "failures": failures,
    }


async def main() -> int:
    parser = argparse.ArgumentParser(description="Run NL2SQL reliability report (non-blocking by default)")
    parser.add_argument(
        "--fixture",
        default="api/tests/fixtures/golden_queries_coverage_v2.json",
        help="Path to golden fixture JSON",
    )
    parser.add_argument("--threshold", type=float, default=0.99, help="Reference reliability target")
    parser.add_argument("--limit", type=int, default=0, help="Optional query limit for quick checks")
    parser.add_argument("--show-failures", type=int, default=10, help="How many failures to print")
    parser.add_argument(
        "--blocking",
        action="store_true",
        help="Fail with non-zero exit code when valid rate is below threshold",
    )
    args = parser.parse_args()

    fixture_path = Path(args.fixture)
    queries = _load_queries(fixture_path)
    result = await _run_eval(queries, limit=args.limit or None)

    rate_pct = result["valid_rate"] * 100
    print(f"golden_reliability_report_v4: valid={result['valid']}/{result['total']} ({rate_pct:.2f}%)")
    print(f"target_valid_rate={args.threshold * 100:.2f}%")

    for fail in result["failures"][: args.show_failures]:
        print(f"FAIL[{fail['index']}]: {fail['question']} :: {fail['error']}")

    if result["valid_rate"] < args.threshold:
        msg = (
            f"Below target: valid_rate {result['valid_rate']:.4f} < threshold {args.threshold:.4f}"
        )
        if args.blocking:
            print(f"Blocking mode: {msg}")
            return 1
        print(f"Non-blocking mode: {msg}")
        return 0

    print(f"Meets target: valid_rate {result['valid_rate']:.4f} >= threshold {args.threshold:.4f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
