"""Generate synthetic bank department feeds + lookups (CSV).

  data/dept_txn.csv     -> combined feed from 5 departments (CARDS/PAYMENTS/...)
  data/customer_dim.csv -> account -> customer + segment (lookup)
  data/fx_rates.csv     -> currency -> rate to base currency (lookup)

Run:  python data/generate_bank_feeds.py
"""
from __future__ import annotations

import csv
import random
from datetime import date, timedelta
from pathlib import Path

DATA = Path(__file__).resolve().parent
DEPTS = ["CARDS", "PAYMENTS", "LOANS", "DEPOSITS", "WEALTH"]
PRODUCTS = ["CURRENT", "SAVINGS", "CREDIT", "MORTGAGE", "INVEST"]
CCYS = ["CAD", "USD", "EUR", "GBP"]
SEGMENTS = ["RETAIL", "MASS", "AFFLUENT", "SME", "CORP"]


def gen_accounts(path: Path, n: int = 800, seed: int = 31) -> None:
    rng = random.Random(seed)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["ACCOUNT_NO", "CUSTOMER_ID", "SEGMENT"])
        for i in range(1, n + 1):
            w.writerow([f"ACC{i:08d}", f"CUST{rng.randint(1, 300):06d}", rng.choice(SEGMENTS)])


def gen_fx(path: Path) -> None:
    rates = {"CAD": 1.0, "USD": 1.36, "EUR": 1.47, "GBP": 1.72}
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["CCY", "FX_RATE"])
        for c, r in rates.items():
            w.writerow([c, r])


def gen_txn(path: Path, n_accounts: int = 800, n: int = 12000, seed: int = 32) -> None:
    rng = random.Random(seed)
    start = date(2026, 1, 1)
    with open(path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(["TXN_ID", "ACCOUNT_NO", "DEPT_CODE", "PRODUCT", "AMOUNT", "CCY", "TXN_DATE", "STATUS"])
        for i in range(1, n + 1):
            d = start + timedelta(days=rng.randrange(31))
            status = "REVERSAL" if rng.random() < 0.06 else "POSTED"
            amount = round(rng.uniform(5, 15000), 2)
            if rng.random() < 0.03:
                amount = round(amount, 2)  # occasional zero handled below
            w.writerow([
                f"T{i:09d}",
                f"ACC{rng.randint(1, n_accounts):08d}",
                rng.choice(DEPTS),
                rng.choice(PRODUCTS),
                amount,
                rng.choice(CCYS),
                d.strftime("%Y-%m-%d"),
                status,
            ])


if __name__ == "__main__":
    gen_accounts(DATA / "customer_dim.csv")
    gen_fx(DATA / "fx_rates.csv")
    gen_txn(DATA / "dept_txn.csv")
    print("wrote", DATA / "customer_dim.csv")
    print("wrote", DATA / "fx_rates.csv")
    print("wrote", DATA / "dept_txn.csv")
