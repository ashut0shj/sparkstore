#!/usr/bin/env python3
"""
tests/security/test_persona_access.py

Acceptance test for capstone requirement 70 ("demonstrate at least three
persona tests proving allowed and denied/masked behavior"). This is a
REAL test, but unlike tests/unit/*, it cannot run in this sandbox or in
GitHub Actions CI, because it has to run AS each persona — i.e. a
teammate actually logged in as (or impersonating, via a PAT scoped to)
a user who is a member of each group, hitting the real Unity Catalog
permissions. A CI runner's service principal is not a regional manager
or a data analyst, so this test is excluded from ci.yaml on purpose
(see docs/TESTING_AND_ACCEPTANCE.md's local-vs-workspace split) and is
instead a documented, repeatable on-workspace acceptance script.

Prerequisites:
  - sql/grants/*.sql has been run for the target catalog.
  - ops.manager_region_assignment has at least one real test row.
  - You have a SQL warehouse HTTP path + personal access token (PAT) for
    EACH persona you're testing, each belonging to a user who is actually
    a member of the corresponding group.
  - pip install --break-git-packages databricks-sql-connector

Usage (run once per persona, swapping --token / --persona):
    python tests/security/test_persona_access.py \\
        --server-hostname <workspace-host> \\
        --http-path <warehouse-http-path> \\
        --token <persona's PAT> \\
        --catalog maven_test \\
        --persona data_analyst
"""

from __future__ import annotations

import argparse
import sys

EXPECTATIONS = {
    "data_analyst": {
        "description": "Read-only Gold, PII masked, financial measures (cost/profit/margin) hidden.",
        "checks": [
            ("SELECT email FROM {catalog}.gold.dim_customer LIMIT 1",
             lambda rows: rows[0][0] == "***REDACTED***", "email must be masked"),
            ("SELECT profit FROM {catalog}.gold.fact_sales LIMIT 1",
             lambda rows: rows[0][0] is None, "profit must be NULL (masked) for a data analyst"),
            ("SELECT COUNT(*) FROM {catalog}.gold.fact_sales",
             lambda rows: rows[0][0] > 0, "must still be able to read fact_sales rows (allowed, just masked columns)"),
            ("SELECT COUNT(*) FROM {catalog}.bronze.transactions",
             None, "must be DENIED (no grant on bronze schema) -- expect this query to raise, not return 0"),
        ],
    },
    "regional_manager": {
        "description": "Read-only Gold, rows restricted to their assigned sales_region only.",
        "checks": [
            ("SELECT DISTINCT sales_region FROM {catalog}.gold.fact_sales",
             lambda rows: len({r[0] for r in rows}) == 1, "must see exactly one sales_region (their own)"),
        ],
    },
    "auditor": {
        "description": "Read-only ops.* only. No business data access at all.",
        "checks": [
            ("SELECT COUNT(*) FROM {catalog}.ops.dq_summary",
             lambda rows: rows[0][0] >= 0, "must be able to read ops.dq_summary"),
            ("SELECT COUNT(*) FROM {catalog}.gold.fact_sales",
             None, "must be DENIED -- auditors have no grant on the gold schema"),
        ],
    },
    "executive": {
        "description": "Read-only Gold, UNMASKED financial measures (exempt from the data_analyst mask).",
        "checks": [
            ("SELECT profit FROM {catalog}.gold.fact_sales WHERE profit IS NOT NULL LIMIT 1",
             lambda rows: rows[0][0] is not None, "profit must NOT be masked for an executive"),
        ],
    },
}


def run_persona_test(server_hostname: str, http_path: str, token: str, catalog: str, persona: str) -> int:
    from databricks import sql as dbsql  # deferred import

    if persona not in EXPECTATIONS:
        print(f"Unknown persona '{persona}'. Known: {list(EXPECTATIONS)}")
        return 2

    spec = EXPECTATIONS[persona]
    print(f"=== Persona test: {persona} ===\n{spec['description']}\n")

    failures = []
    with dbsql.connect(server_hostname=server_hostname, http_path=http_path, access_token=token) as conn:
        for query_template, check, expectation_text in spec["checks"]:
            query = query_template.format(catalog=catalog)
            print(f"-> {query}\n   expecting: {expectation_text}")
            try:
                with conn.cursor() as cur:
                    cur.execute(query)
                    rows = cur.fetchall()
                if check is None:
                    # a query we expect to be DENIED actually succeeded -- that's a failure
                    failures.append(f"Expected DENIAL for: {query}, but it returned {len(rows)} row(s).")
                    print("   RESULT: FAIL (expected a permission error, got data back)\n")
                    continue
                passed = check(rows)
                print(f"   RESULT: {'PASS' if passed else 'FAIL'}\n")
                if not passed:
                    failures.append(f"Check failed for: {query} (got {rows[:3]})")
            except Exception as exc:  # noqa: BLE001
                if check is None:
                    print(f"   RESULT: PASS (denied as expected: {exc})\n")
                else:
                    failures.append(f"Unexpected error for: {query}: {exc}")
                    print(f"   RESULT: FAIL (unexpected error: {exc})\n")

    print(f"=== {persona}: {len(spec['checks']) - len(failures)}/{len(spec['checks'])} checks passed ===")
    if failures:
        print("Failures:")
        for f in failures:
            print(f"  - {f}")
        return 1
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--server-hostname", required=True)
    parser.add_argument("--http-path", required=True)
    parser.add_argument("--token", required=True)
    parser.add_argument("--catalog", default="maven_test")
    parser.add_argument("--persona", required=True, choices=list(EXPECTATIONS))
    args = parser.parse_args(argv)
    return run_persona_test(args.server_hostname, args.http_path, args.token, args.catalog, args.persona)


if __name__ == "__main__":
    sys.exit(main())
