"""
test_firewall.py — Generates 3 test CSVs and runs them through the firewall pipeline.

CSV 1: clean_data.csv      — no issues   → should PASS
CSV 2: null_data.csv       — has nulls   → should REJECT
CSV 3: outlier_data.csv    — has outliers → should REJECT
"""

import sys
from pathlib import Path

# Make sure local modules are importable
sys.path.insert(0, str(Path(__file__).parent))

import numpy as np
import pandas as pd

from db import init_db
from firewall import DB_PATH, INCOMING_DIR, process_csv

# ── Seed for reproducibility ──────────────────────────────────────────────────
rng = np.random.default_rng(42)


def make_clean_csv() -> str:
    path = INCOMING_DIR / "clean_data.csv"
    df = pd.DataFrame({
        "age":    rng.integers(20, 60, 100),
        "salary": rng.normal(55000, 8000, 100).round(2),
        "score":  rng.uniform(0, 100, 100).round(2),
        "name":   [f"User_{i}" for i in range(100)],
    })
    df.to_csv(path, index=False)
    return str(path)


def make_null_csv() -> str:
    path = INCOMING_DIR / "null_data.csv"
    df = pd.DataFrame({
        "age":    rng.integers(20, 60, 80).tolist() + [None] * 20,
        "salary": [None if i % 5 == 0 else round(rng.normal(55000, 8000), 2) for i in range(100)],
        "city":   [None if i % 7 == 0 else f"City_{i}" for i in range(100)],
    })
    df.to_csv(path, index=False)
    return str(path)


def make_outlier_csv() -> str:
    path = INCOMING_DIR / "outlier_data.csv"
    salaries = rng.normal(55000, 5000, 97).round(2).tolist()
    salaries += [999999.0, -50000.0, 1200000.0]   # extreme outliers

    ages = rng.integers(25, 55, 98).tolist()
    ages += [150, 200]                              # impossible ages

    df = pd.DataFrame({
        "salary": salaries,
        "age":    ages,
        "score":  rng.uniform(0, 100, 100).round(2),
    })
    df.to_csv(path, index=False)
    return str(path)


# ─────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    init_db(DB_PATH)

    print("=" * 56)
    print("  DATA QUALITY FIREWALL — TEST RUN")
    print("=" * 56)

    test_files = [
        ("CLEAN",   make_clean_csv()),
        ("NULLS",   make_null_csv()),
        ("OUTLIERS",make_outlier_csv()),
    ]

    results = []
    for label, fpath in test_files:
        print(f"\n▶  Processing [{label}]  {Path(fpath).name} ...")
        result = process_csv(fpath)
        results.append((label, result))

    print("\n" + "=" * 56)
    print("  SUMMARY")
    print("=" * 56)
    for label, r in results:
        status = "✅ PASSED" if r["passed"] else "❌ REJECTED"
        issues = len(r["null_issues"]) + len(r["outlier_issues"])
        err    = f"  ERROR: {r['error']}" if r.get("error") else ""
        print(f"  {status}  [{label:8s}]  {r['filename']}  ({issues} issue(s)){err}")

    print(f"\n  PDF reports → {Path('reports/').resolve()}")
    print(f"  SQLite log  → {DB_PATH}")
    print("=" * 56)