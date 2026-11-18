#!/usr/bin/env python3
"""
gen_core_banking.py - seeded, reproducible synthetic Kenyan core-banking data.

Writes daily batch files (one folder per day: dt=YYYY-MM-DD) for 365 days (configurable):

    branches/dt=*/branches_*.csv                (reference, written on day 0 only)
    customers/dt=*/customers_*.csv              (CDC: op, change_ts + all columns)
    accounts/dt=*/accounts_*.csv                (CDC: op, change_ts + all columns)
    transactions/dt=*/transactions_*.jsonl      (append-only, late arrivals/reversals/corrections)
    loans/dt=*/loans_*.parquet                  (CDC: op, change_ts + all columns)
    loan_repayments/dt=*/loan_repayments_*.parquet
    defects_manifest.csv                        (ground truth for every injected defect)
    run_config.json                             (parameters needed to reproduce the run)

Design notes
------------
* Two independent random streams are derived from --seed: one drives the *clean* simulation,
  one drives defect injection. Changing --defect-rate therefore never changes the clean data.
* Output is fully deterministic for a given (seed, start-date, days, scale, defect settings).
* Clean timestamps are ISO-8601 in EAT with an explicit offset (+03:00); messiness is injected
  on top. Phone numbers are canonical +2547XXXXXXXX / +2541XXXXXXXX.
* Manifest `field` always uses canonical (pre-drift) column names, e.g. `rate` even after it is
  renamed to `interest_rate` in the files.
* Customer phone changes are emitted as customer CDC rows (accounts has no phone column).
* Hard deletes (op='D') only hit closed accounts with no loan history; their historic
  transactions stay in place on purpose (that is what a hard delete does to child tables).
* Each cell receives at most one defect, so the manifest is an unambiguous per-cell ground truth.
  Duplicate rows are copies of the final (possibly defective) row.

Examples
--------
    python gen_core_banking.py --scale 0.05 --out-dir ./out_dev            # quick dev run
    python gen_core_banking.py --scale 1.0 --seed 7 --out-dir ./out_full   # full run
    python gen_core_banking.py --defect-rate 0.04 --rate duplicate_row=0.05 --out-dir ./out
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from collections import Counter, deque
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Deque, Dict, List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd
from faker import Faker

LOG = logging.getLogger("gen_core_banking")

# =============================================================================
# Constants and reference data
# =============================================================================
TS_OUT_FMT = "%Y-%m-%dT%H:%M:%S+03:00"  # clean timestamp, Africa/Nairobi (EAT, UTC+3)
TS_PARSE_FMT = "%Y-%m-%dT%H:%M:%S%z"
DATE_OUT_FMT = "%Y-%m-%d"
KES_PER_USD = 130.0

SEGMENTS = ("retail", "SME", "corporate")
SEG_P = np.array([0.88, 0.10, 0.02])
SEG_AMOUNT_FACTOR = {"retail": 1.0, "SME": 3.0, "corporate": 10.0}
PRODUCTS = ("savings", "current", "fixed_deposit")
PRODUCT_P = {
    "retail": [0.80, 0.17, 0.03],
    "SME": [0.30, 0.65, 0.05],
    "corporate": [0.05, 0.85, 0.10],
}
PRODUCT_ACTIVITY = {"savings": 1.0, "current": 1.8, "fixed_deposit": 0.05}
OVERDRAFT_LEVELS = {
    "retail": [0, 0, 10_000, 20_000, 50_000],
    "SME": [0, 100_000, 250_000, 500_000, 1_000_000],
    "corporate": [0, 1_000_000, 2_500_000, 5_000_000, 10_000_000],
}
BALANCE_LOG_MEAN = {"retail": 9.5, "SME": 12.0, "corporate": 14.0}
TIER_P = {"retail": [0.70, 0.25, 0.05], "SME": [0.30, 0.50, 0.20], "corporate": [0.05, 0.35, 0.60]}

CHANNELS = ("M-Pesa", "ATM", "mobile app", "branch", "card POS", "PesaLink")
CHANNEL_P = np.array([0.38, 0.12, 0.22, 0.08, 0.14, 0.06])
DR_SHARE = np.array([0.58, 0.97, 0.60, 0.45, 0.97, 0.50])
CHANNEL_CAP = np.array([250_000, 40_000, 1_000_000, 5_000_000, 500_000, 999_999], dtype=float)
AMOUNT_LOGNORMAL = {  # (mu, sigma) of ln(KES)
    "M-Pesa": (6.6, 1.1),
    "mobile app": (7.6, 1.2),
    "branch": (9.4, 1.2),
    "card POS": (7.3, 1.0),
    "PesaLink": (9.0, 1.1),
}
WHOLE_SHILLING_CHANNELS = {"M-Pesa", "ATM", "branch"}
REF_PREFIX = {"M-Pesa": "", "ATM": "ATM", "mobile app": "APP", "branch": "BRN", "card POS": "POS", "PesaLink": "PLK"}
TXN_TYPES = {
    ("M-Pesa", "DR"): (["SEND_MONEY", "PAYBILL", "BUY_GOODS", "AIRTIME", "WITHDRAW_AGENT"], [0.28, 0.24, 0.26, 0.10, 0.12]),
    ("M-Pesa", "CR"): (["RECEIVE_MONEY", "MPESA_DEPOSIT"], [0.7, 0.3]),
    ("ATM", "DR"): (["ATM_WITHDRAWAL"], [1.0]),
    ("ATM", "CR"): (["ATM_DEPOSIT"], [1.0]),
    ("mobile app", "DR"): (["TRANSFER_OUT", "BILL_PAYMENT", "AIRTIME"], [0.5, 0.35, 0.15]),
    ("mobile app", "CR"): (["TRANSFER_IN"], [1.0]),
    ("branch", "DR"): (["CASH_WITHDRAWAL", "CHEQUE_PAYMENT"], [0.8, 0.2]),
    ("branch", "CR"): (["CASH_DEPOSIT", "CHEQUE_DEPOSIT"], [0.8, 0.2]),
    ("card POS", "DR"): (["POS_PURCHASE"], [1.0]),
    ("card POS", "CR"): (["POS_REFUND"], [1.0]),
    ("PesaLink", "DR"): (["PESALINK_OUT"], [1.0]),
    ("PesaLink", "CR"): (["PESALINK_IN"], [1.0]),
}
TYPE_POOL = {
    "SEND_MONEY": "person",
    "RECEIVE_MONEY": "person",
    "PAYBILL": "biller",
    "BILL_PAYMENT": "biller",
    "BUY_GOODS": "merchant",
    "POS_PURCHASE": "merchant",
    "POS_REFUND": "merchant",
    "AIRTIME": "airtime",
    "WITHDRAW_AGENT": "agent",
    "MPESA_DEPOSIT": "agent",
    "ATM_WITHDRAWAL": "atm",
    "ATM_DEPOSIT": "atm",
    "CASH_WITHDRAWAL": "counter",
    "CASH_DEPOSIT": "counter",
    "CHEQUE_PAYMENT": "person",
    "CHEQUE_DEPOSIT": "person",
    "TRANSFER_OUT": "person",
    "TRANSFER_IN": "person",
    "PESALINK_OUT": "person",
    "PESALINK_IN": "person",
    "OPENING_DEPOSIT": "opening",
    "LOAN_DISBURSEMENT": "disbursement",
    "LOAN_REPAYMENT": "repayment",
}
NON_REVERSIBLE_TYPES = ["OPENING_DEPOSIT", "LOAN_DISBURSEMENT", "LOAN_REPAYMENT"]
HOUR_WEIGHTS = np.array([1, 1, 1, 1, 1, 2, 4, 8, 10, 10, 9, 9, 10, 9, 8, 8, 9, 10, 9, 7, 5, 3, 2, 1.5])
HOUR_P = HOUR_WEIGHTS / HOUR_WEIGHTS.sum()

STATUS_TRANSITIONS = {
    "active": (["dormant", "frozen", "closed"], [0.50, 0.20, 0.30]),
    "dormant": (["active", "frozen", "closed"], [0.40, 0.15, 0.45]),
    "frozen": (["active", "closed"], [0.60, 0.40]),
}

LOAN_PARAMS = {  # mu, sigma, min, max, rate_lo, rate_hi
    "retail": (np.log(150_000), 0.9, 10_000, 2_000_000, 14.0, 26.0),
    "SME": (np.log(1_500_000), 0.8, 200_000, 15_000_000, 12.0, 20.0),
    "corporate": (np.log(15_000_000), 0.7, 2_000_000, 80_000_000, 9.0, 16.0),
}
TENORS = [6, 12, 18, 24, 36, 48, 60]
TENOR_P = [0.03, 0.12, 0.15, 0.25, 0.20, 0.15, 0.10]

# (branch name, county, weight) - every customer county therefore has at least one branch.
BRANCH_DEFS = [
    ("Nairobi CBD", "Nairobi", 10),
    ("Westlands", "Nairobi", 6),
    ("Upper Hill", "Nairobi", 4),
    ("Industrial Area", "Nairobi", 3),
    ("Karen", "Nairobi", 2),
    ("Eastleigh", "Nairobi", 3),
    ("Kikuyu", "Kiambu", 2),
    ("Thika", "Kiambu", 3),
    ("Ruiru", "Kiambu", 2),
    ("Mombasa Moi Avenue", "Mombasa", 6),
    ("Nyali", "Mombasa", 2),
    ("Malindi", "Kilifi", 1.5),
    ("Voi", "Taita Taveta", 1),
    ("Kisumu", "Kisumu", 4),
    ("Nakuru", "Nakuru", 5),
    ("Naivasha", "Nakuru", 1.5),
    ("Eldoret", "Uasin Gishu", 4),
    ("Kitale", "Trans Nzoia", 2),
    ("Kakamega", "Kakamega", 3),
    ("Mumias", "Kakamega", 1),
    ("Bungoma", "Bungoma", 2.5),
    ("Busia", "Busia", 1.5),
    ("Vihiga", "Vihiga", 1),
    ("Kisii", "Kisii", 2.5),
    ("Migori", "Migori", 1.5),
    ("Homa Bay", "Homa Bay", 1.5),
    ("Siaya", "Siaya", 1.5),
    ("Kericho", "Kericho", 2),
    ("Bomet", "Bomet", 1),
    ("Nyeri", "Nyeri", 2.5),
    ("Nanyuki", "Laikipia", 1.5),
    ("Meru", "Meru", 2.5),
    ("Embu", "Embu", 2),
    ("Machakos", "Machakos", 3),
    ("Kitui", "Kitui", 1.5),
    ("Ngong", "Kajiado", 2),
    ("Narok", "Narok", 1.5),
    ("Garissa", "Garissa", 1.5),
    ("Lodwar", "Turkana", 0.8),
    ("Nyahururu", "Nyandarua", 1),
]
FIRST_NAMES = [
    "James",
    "John",
    "Peter",
    "Joseph",
    "Samuel",
    "David",
    "Daniel",
    "Paul",
    "Patrick",
    "George",
    "Francis",
    "Kevin",
    "Brian",
    "Dennis",
    "Collins",
    "Victor",
    "Elijah",
    "Moses",
    "Isaac",
    "Felix",
    "Mary",
    "Grace",
    "Faith",
    "Esther",
    "Ruth",
    "Jane",
    "Lucy",
    "Susan",
    "Alice",
    "Mercy",
    "Purity",
    "Caroline",
    "Beatrice",
    "Joyce",
    "Nancy",
    "Sharon",
    "Winnie",
    "Cynthia",
    "Agnes",
    "Rose",
    "Hassan",
    "Amina",
    "Fatuma",
    "Juma",
    "Omar",
    "Salim",
    "Zainab",
    "Abdi",
]
SURNAMES = [
    "Kamau",
    "Wanjiku",
    "Otieno",
    "Odhiambo",
    "Mutua",
    "Kiprop",
    "Kipchoge",
    "Wafula",
    "Barasa",
    "Mwangi",
    "Njoroge",
    "Achieng",
    "Akinyi",
    "Ochieng",
    "Onyango",
    "Kariuki",
    "Kimani",
    "Mutiso",
    "Nyongesa",
    "Simiyu",
    "Cheruiyot",
    "Koech",
    "Rotich",
    "Langat",
    "Mohamed",
    "Ali",
    "Hussein",
    "Abdullahi",
    "Mwende",
    "Wambui",
    "Nduta",
    "Chebet",
    "Jepkorir",
    "Okello",
    "Omondi",
    "Wekesa",
    "Makokha",
    "Ndung'u",
    "Gitau",
    "Maina",
    "Ngugi",
    "Nyaga",
    "Mbugua",
    "Muriuki",
    "Kilonzo",
    "Musyoka",
    "Masinde",
    "Were",
    "Awuor",
    "Atieno",
]
BUSINESS_WORDS = ["Traders", "Enterprises", "Logistics", "Agencies", "Supplies", "Investments", "Holdings", "Hardware", "Distributors", "Farmers"]
MERCHANTS = [
    "NAIVAS SUPERMARKET",
    "QUICKMART",
    "CARREFOUR",
    "SHELL",
    "TOTAL ENERGIES",
    "JAVA HOUSE",
    "KFC",
    "CHANDARANA",
    "TUSKYS",
    "OILIBYA",
    "RUBIS",
    "UBER",
    "BOLT",
    "JUMIA",
    "GLOVO",
    "ARTCAFFE",
    "LITTLE CAB",
    "DECATHLON",
]
BILLERS = ["KPLC PREPAID", "KPLC POSTPAID", "NAIROBI WATER", "DSTV", "ZUKU", "SAFARICOM HOME FIBRE", "NHIF", "NSSF", "KRA", "STARTIMES", "GOTV"]
EMAIL_BIZ_DOMAIN = ".co.ke"

# Orphan FK id shapes: column -> (prefix, width, base, span). Bases sit far above real id ranges.
ORPHAN_ID = {
    "customer_id": ("C", 7, 9_000_000, 99_999),
    "account_id": ("AC", 9, 900_000_000, 99_999),
    "branch_code": ("BR", 3, 900, 99),
    "loan_id": ("LN", 8, 90_000_000, 99_999),
}

DEFECT_MULTIPLIERS = {  # rate = --defect-rate * multiplier (default 1%..5%)
    "duplicate_row": 1.0,
    "null_required": 1.5,
    "orphan_fk": 0.5,
    "mixed_date_format": 2.0,
    "phone_variant": 2.5,
    "name_format": 1.5,
    "amount_string": 1.0,
    "currency_variant": 1.5,
    "nonpositive_amount": 0.5,
    "out_of_order_update": 0.5,
    "test_record": 0.5,
}
TS_VARIANTS = ("utc_iso_z", "eat_naive_dmy", "us_12h_naive", "epoch_seconds", "utc_naive_space", "compact_eat")
DATE_VARIANTS = ("dmy_slash", "dmy_mon", "compact", "mdy_slash")
CURRENCY_VARIANTS = {"KES": ["Ksh", "KSH", "Kes"], "USD": ["usd", "Usd", "US$"]}
MANIFEST_COLUMNS = [
    "defect_id",
    "category",
    "dt",
    "table",
    "file",
    "entity_key",
    "row_ref",
    "field",
    "defect_type",
    "detail",
    "original_value",
    "injected_value",
]
POOL_DAYS = 3  # how many days back a reversal/correction may reach


@dataclass(frozen=True)
class SimParams:
    """Clean-simulation knobs (full-scale targets are multiplied by --scale)."""

    customers: int = 20_000
    accounts: int = 30_000
    loans: int = 5_000
    txns_total: int = 3_000_000  # per 365 days
    new_account_rate: float = 0.001  # of live accounts, per day
    customer_growth_share: float = 0.10  # share of final customers created during the run
    update_rate: float = 0.01  # account update events per live account per day
    delete_rate: float = 0.0001  # hard deletes per live account per day
    usd_share_non_primary: float = 0.25
    late_rate: float = 0.02  # txns arriving 1-3 days after txn_ts
    reversal_rate: float = 0.004
    correction_rate: float = 0.003
    fail_rate: float = 0.015
    new_loan_share: float = 0.20
    p_miss: float = 0.24  # monthly P(current -> overdue), x per-loan risk
    p_extra_payment: float = 0.022  # daily P of an ad-hoc part-payment (current/overdue loans)
    p_cure: float = 0.45
    p_recover: float = 0.08
    p_writeoff: float = 0.04


SIM = SimParams()


# =============================================================================
# Small helpers (defined before everything that calls them)
# =============================================================================
def format_ts(day, offsets_sec) -> np.ndarray:
    """EAT ISO timestamps for `day` (a date or datetime64[D] array) plus second offsets."""
    offsets = np.asarray(offsets_sec, dtype="int64").astype("timedelta64[s]")
    if isinstance(day, date):
        base = np.datetime64(day.isoformat(), "s")
    else:
        base = np.asarray(day).astype("datetime64[D]").astype("datetime64[s]")
    stamps = base + offsets
    return np.asarray(pd.DatetimeIndex(stamps).strftime(TS_OUT_FMT), dtype=object)


def format_dates(ordinals) -> np.ndarray:
    return np.array([date.fromordinal(int(o)).isoformat() for o in ordinals], dtype=object)


_CODE_ALPHABET = np.array(list("ABCDEFGHJKLMNPQRSTUVWXYZ23456789"))


def random_codes(rng: np.random.Generator, n: int, length: int) -> np.ndarray:
    if n == 0:
        return np.empty(0, dtype=object)
    idx = rng.integers(0, len(_CODE_ALPHABET), size=(n, length))
    packed = np.ascontiguousarray(_CODE_ALPHABET[idx]).view(f"<U{length}").ravel()
    return packed.astype(object)


def annuity_installment(principal: float, annual_rate_pct: float, tenor: int) -> float:
    r = annual_rate_pct / 1200.0
    if r == 0:
        return principal / tenor
    return principal * r / (1.0 - (1.0 + r) ** -tenor)


def balance_after_k(principal: float, annual_rate_pct: float, tenor: int, k: int) -> float:
    r = annual_rate_pct / 1200.0
    a = annuity_installment(principal, annual_rate_pct, tenor)
    if r == 0:
        return max(0.0, principal - a * k)
    return max(0.0, principal * (1 + r) ** k - a * ((1 + r) ** k - 1) / r)


def apply_payment(outstanding: float, amount: float, annual_rate_pct: float) -> float:
    interest = outstanding * annual_rate_pct / 1200.0
    return max(0.0, outstanding - max(0.0, amount - interest))


def running_balance(acct_sorted: np.ndarray, signed: np.ndarray, start_per_row: np.ndarray) -> np.ndarray:
    """Per-account running balance; rows sorted by (account, posting order).
    `start_per_row` is each row's account balance at the start of the day."""
    n = signed.size
    cs = np.cumsum(signed)
    first = np.ones(n, dtype=bool)
    first[1:] = acct_sorted[1:] != acct_sorted[:-1]
    starts = np.flatnonzero(first)
    lengths = np.diff(np.append(starts, n))
    base = cs[starts] - signed[starts]
    return start_per_row + (cs - np.repeat(base, lengths))


def value_to_str(v) -> str:
    if v is None or (isinstance(v, float) and v != v):
        return "<null>"
    if isinstance(v, str):
        return "<empty>" if v == "" else v
    return str(v)


def _json_default(o):
    if isinstance(o, np.integer):
        return int(o)
    if isinstance(o, np.floating):
        return float(o)
    if isinstance(o, np.bool_):
        return bool(o)
    return str(o)


def write_jsonl(df: pd.DataFrame, path: Path) -> None:
    cols = list(df.columns)
    arrays = [df[c].to_numpy(dtype=object) for c in cols]
    with open(path, "w", encoding="utf-8", newline="\n") as fh:
        for row in zip(*arrays):
            fh.write(json.dumps(dict(zip(cols, row)), default=_json_default, ensure_ascii=False, separators=(",", ":")))
            fh.write("\n")


# =============================================================================
# Growable in-memory column store (live state of customers / accounts / loans)
# =============================================================================
class ColumnStore:
    def __init__(self, dtypes: Dict[str, str], capacity: int = 1024):
        self.n = 0
        self._cap = max(capacity, 16)
        self.cols = {k: np.empty(self._cap, dtype=v) for k, v in dtypes.items()}

    def _grow(self, need: int) -> None:
        if need <= self._cap:
            return
        new_cap = max(need, int(self._cap * 1.5))
        for k, arr in self.cols.items():
            bigger = np.empty(new_cap, dtype=arr.dtype)
            bigger[: self.n] = arr[: self.n]
            self.cols[k] = bigger
        self._cap = new_cap

    def append(self, data: Dict[str, Sequence]) -> np.ndarray:
        m = len(next(iter(data.values())))
        self._grow(self.n + m)
        for k in self.cols:
            self.cols[k][slice(self.n, self.n + m)] = data[k]
        idx = np.arange(self.n, self.n + m)
        self.n += m
        return idx

    def __getitem__(self, key: str) -> np.ndarray:
        return self.cols[key][: self.n]

    def frame(self, idx: np.ndarray, columns: Sequence[str]) -> pd.DataFrame:
        return pd.DataFrame({c: self.cols[c][idx] for c in columns})


def cdc_frame(store: ColumnStore, columns: Sequence[str], idx: np.ndarray, op: str, change_ts: np.ndarray) -> pd.DataFrame:
    df = store.frame(idx, columns)
    df.insert(0, "change_ts", change_ts)
    df.insert(0, "op", op)
    return df


# =============================================================================
# Table specs (what each table looks like and which defects may touch it)
# =============================================================================
@dataclass(frozen=True)
class TableSpec:
    name: str
    fmt: str  # csv | jsonl | parquet
    pk: str
    columns: Tuple[str, ...]
    cdc: bool = False
    extras: Tuple[str, ...] = ()  # carried through but not defect targets (drift column)
    required: Tuple[str, ...] = ()
    fks: Tuple[str, ...] = ()
    ts_cols: Tuple[str, ...] = ()
    date_cols: Tuple[str, ...] = ()
    phone_cols: Tuple[str, ...] = ()
    name_cols: Tuple[str, ...] = ()
    amount_str_cols: Tuple[str, ...] = ()
    currency_cols: Tuple[str, ...] = ()
    nonpos_cols: Tuple[str, ...] = ()
    null_variants: Tuple = ("", "NULL", "N/A")
    id_prefix: str = ""
    test_overrides: Dict[str, object] = field(default_factory=dict)
    dtypes: Dict[str, str] = field(default_factory=dict)  # parquet column dtypes

    @property
    def ext(self) -> str:
        return {"csv": "csv", "jsonl": "jsonl", "parquet": "parquet"}[self.fmt]

    @property
    def frame_columns(self) -> List[str]:
        return (["op", "change_ts"] if self.cdc else []) + list(self.columns) + list(self.extras)


BRANCH_COLS = ("branch_code", "name", "county")
CUSTOMER_COLS = ("customer_id", "full_name", "national_id", "dob", "phone", "email", "county", "segment", "kyc_status", "created_at")
ACCOUNT_COLS = ("account_id", "customer_id", "product_type", "branch_code", "currency", "status", "overdraft_limit", "opened_at")
TXN_COLS = (
    "txn_id",
    "account_id",
    "txn_ts",
    "amount",
    "currency",
    "direction",
    "channel",
    "txn_type",
    "counterparty",
    "balance_after",
    "status",
    "reference",
)
LOAN_COLS = ("loan_id", "customer_id", "account_id", "principal", "rate", "tenor_months", "disbursed_at", "status", "days_past_due", "outstanding")
REPAYMENT_COLS = ("repayment_id", "loan_id", "paid_at", "amount")

SPECS: Dict[str, TableSpec] = {
    "branches": TableSpec(
        "branches",
        "csv",
        "branch_code",
        BRANCH_COLS,
        required=("name", "county"),
        name_cols=("name",),
        id_prefix="BR",
        test_overrides={"name": "TEST BRANCH"},
    ),
    "customers": TableSpec(
        "customers",
        "csv",
        "customer_id",
        CUSTOMER_COLS,
        cdc=True,
        required=("full_name", "national_id", "dob", "phone", "county", "segment", "kyc_status", "created_at"),
        ts_cols=("created_at", "change_ts"),
        date_cols=("dob",),
        phone_cols=("phone",),
        name_cols=("full_name",),
        id_prefix="C",
        test_overrides={"full_name": "TEST USER", "national_id": "00000000", "phone": "+254700000000", "email": "test@test.com"},
    ),
    "accounts": TableSpec(
        "accounts",
        "csv",
        "account_id",
        ACCOUNT_COLS,
        cdc=True,
        extras=("account_tier",),
        required=("customer_id", "product_type", "branch_code", "currency", "status", "opened_at"),
        fks=("customer_id", "branch_code"),
        ts_cols=("opened_at", "change_ts"),
        amount_str_cols=("overdraft_limit",),
        currency_cols=("currency",),
        id_prefix="AC",
        test_overrides={"overdraft_limit": 0.0},
    ),
    "transactions": TableSpec(
        "transactions",
        "jsonl",
        "txn_id",
        TXN_COLS,
        required=("account_id", "txn_ts", "amount", "currency", "direction", "channel", "txn_type", "status"),
        fks=("account_id",),
        ts_cols=("txn_ts",),
        name_cols=("counterparty",),
        amount_str_cols=("amount",),
        currency_cols=("currency",),
        nonpos_cols=("amount",),
        null_variants=(None, ""),
        id_prefix="TXN",
        test_overrides={"reference": "TEST", "counterparty": "DUMMY", "amount": 1.0},
    ),
    "loans": TableSpec(
        "loans",
        "parquet",
        "loan_id",
        LOAN_COLS,
        cdc=True,
        required=("customer_id", "account_id", "principal", "rate", "tenor_months", "disbursed_at", "status"),
        fks=("customer_id", "account_id"),
        ts_cols=("disbursed_at", "change_ts"),
        nonpos_cols=("principal",),
        null_variants=(None,),
        id_prefix="LN",
        test_overrides={"principal": 1.0},
        dtypes={"principal": "float64", "rate": "float64", "tenor_months": "Int64", "days_past_due": "Int64", "outstanding": "float64"},
    ),
    "loan_repayments": TableSpec(
        "loan_repayments",
        "parquet",
        "repayment_id",
        REPAYMENT_COLS,
        required=("loan_id", "paid_at", "amount"),
        fks=("loan_id",),
        ts_cols=("paid_at",),
        nonpos_cols=("amount",),
        null_variants=(None,),
        id_prefix="RP",
        test_overrides={"amount": 1.0},
        dtypes={"amount": "float64"},
    ),
}


def cast_parquet(spec: TableSpec, df: pd.DataFrame) -> pd.DataFrame:
    """Give every parquet column a stable dtype so partitions share one schema."""
    out = {}
    for c in df.columns:
        kind = spec.dtypes.get(c, "string")
        s = df[c]
        if kind == "float64":
            out[c] = pd.to_numeric(s, errors="coerce").astype("float64")
        elif kind == "Int64":
            out[c] = pd.to_numeric(s, errors="coerce").astype("Int64")
        else:
            out[c] = s.astype("string")
    return pd.DataFrame(out, columns=list(df.columns))


def write_frame(spec: TableSpec, df: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if spec.fmt == "csv":
        df.to_csv(path, index=False)
    elif spec.fmt == "jsonl":
        write_jsonl(df, path)
    else:
        df.to_parquet(path, index=False, engine="pyarrow")


# =============================================================================
# Defect injection
# =============================================================================
def resolve_rates(defect_rate: float, overrides: Dict[str, float]) -> Dict[str, float]:
    rates = {k: min(1.0, defect_rate * m) for k, m in DEFECT_MULTIPLIERS.items()}
    for k, v in overrides.items():
        if k not in rates:
            raise ValueError(f"unknown defect type '{k}'; valid: {sorted(rates)}")
        if not 0.0 <= v <= 1.0:
            raise ValueError(f"rate for {k} must be in [0, 1]")
        rates[k] = v
    return rates


class DefectInjector:
    """Injects messiness into one daily frame and logs ground truth to the manifest."""

    def __init__(self, rng: np.random.Generator, rates: Dict[str, float], manifest_path: Path):
        self.rng = rng
        self.rates = rates
        self._rows: List[tuple] = []
        self._seq = 0
        self._test_seq: Counter = Counter()
        self.held: Dict[str, tuple] = {}
        self._fh = open(manifest_path, "w", newline="", encoding="utf-8")
        self._writer = csv.writer(self._fh)
        self._writer.writerow(MANIFEST_COLUMNS)
        self.total_logged = 0

    # ---- manifest ----------------------------------------------------------------------
    def log(self, category: str, dt: str, table: str, file: str, keys, refs, field_name: str, defect_type: str, details, originals, injected) -> None:
        for k, r, det, o, i in zip(keys, refs, details, originals, injected):
            self._rows.append((category, dt, table, file, k, r, field_name, defect_type, det, value_to_str(o), value_to_str(i)))

    def flush(self) -> None:
        for row in self._rows:
            self._seq += 1
            self._writer.writerow((f"D{self._seq:09d}",) + row)
        self.total_logged += len(self._rows)
        self._rows.clear()
        self._fh.flush()

    def close(self) -> None:
        self.flush()
        self._fh.close()

    # ---- row selection helper ----------------------------------------------------------
    def _pick(self, n: int, rate: float) -> np.ndarray:
        if n == 0 or rate <= 0:
            return np.empty(0, dtype=int)
        return np.flatnonzero(self.rng.random(n) < rate)

    # ---- value transforms (each returns (new_values, per-row detail)) -------------------
    def _t_null(self, spec: TableSpec, col: str, old: np.ndarray):
        variants = spec.null_variants
        pick = self.rng.integers(0, len(variants), old.size)
        new = np.empty(old.size, dtype=object)
        detail = []
        for i, p in enumerate(pick):
            v = variants[p]
            new[i] = v
            detail.append("null" if v is None else ("empty" if v == "" else f"literal:{v}"))
        return new, detail

    def _t_orphan(self, spec: TableSpec, col: str, old: np.ndarray):
        prefix, width, base, span = ORPHAN_ID[col]
        nums = base + self.rng.integers(0, span, old.size)
        new = np.array([f"{prefix}{x:0{width}d}" for x in nums], dtype=object)
        return new, ["nonexistent_id"] * old.size

    def _t_datetime(self, spec: TableSpec, col: str, old: np.ndarray):
        n = old.size
        series = pd.Series(old.tolist(), dtype=object)
        out = np.empty(n, dtype=object)
        if col in spec.date_cols:
            parsed = pd.to_datetime(series, format=DATE_OUT_FMT)
            variants = DATE_VARIANTS
            pick = self.rng.integers(0, len(variants), n)
            fmts = {"dmy_slash": "%d/%m/%Y", "dmy_mon": "%d-%b-%Y", "compact": "%Y%m%d", "mdy_slash": "%m/%d/%Y"}
            for vi, name in enumerate(variants):
                m = pick == vi
                if m.any():
                    out[m] = parsed[m].dt.strftime(fmts[name]).to_numpy(dtype=object)
        else:
            utc = pd.to_datetime(series, format=TS_PARSE_FMT, utc=True).dt.tz_localize(None)
            eat = utc + pd.Timedelta(hours=3)
            variants = TS_VARIANTS
            pick = self.rng.integers(0, len(variants), n)
            for vi, name in enumerate(variants):
                m = pick == vi
                if not m.any():
                    continue
                if name == "utc_iso_z":
                    vals = utc[m].dt.strftime("%Y-%m-%dT%H:%M:%SZ")
                elif name == "eat_naive_dmy":
                    vals = eat[m].dt.strftime("%d/%m/%Y %H:%M:%S")
                elif name == "us_12h_naive":
                    vals = eat[m].dt.strftime("%m/%d/%Y %I:%M:%S %p")
                elif name == "epoch_seconds":
                    vals = ((utc[m] - pd.Timestamp("1970-01-01")) // pd.Timedelta(seconds=1)).astype(str)
                elif name == "utc_naive_space":
                    vals = utc[m].dt.strftime("%Y-%m-%d %H:%M:%S")
                else:
                    vals = eat[m].dt.strftime("%Y%m%d%H%M%S")
                out[m] = vals.to_numpy(dtype=object)
        return out, [variants[p] for p in pick]

    def _t_phone(self, spec: TableSpec, col: str, old: np.ndarray):
        kind = self.rng.integers(0, 3, old.size)
        new = np.empty(old.size, dtype=object)
        names = ("local_0", "intl_no_plus", "intl_spaced")
        for i, (v, k) in enumerate(zip(old, kind)):
            s = str(v)
            if not s.startswith("+254") or len(s) != 13:
                new[i], kind[i] = s, 0
            elif k == 0:
                new[i] = "0" + s[4:]
            elif k == 1:
                new[i] = s[1:]
            else:
                new[i] = f"+254 {s[4:7]} {s[7:10]} {s[10:]}"
        return new, [names[k] for k in kind]

    def _t_name(self, spec: TableSpec, col: str, old: np.ndarray):
        kind = self.rng.integers(0, 4, old.size)
        names = ("upper", "lower", "pad_whitespace", "double_space")
        new = np.empty(old.size, dtype=object)
        detail = []
        for i, (v, k) in enumerate(zip(old, kind)):
            s = str(v)
            cand = s.upper() if k == 0 else s.lower() if k == 1 else s
            if k == 3 and " " in s:
                cand = s.replace(" ", "  ", 1)
            if cand == s:  # transform was a no-op: pad instead
                cand, k = " " * int(self.rng.integers(1, 4)) + s + " " * int(self.rng.integers(0, 3)), 2
            elif k == 2:
                cand = " " * int(self.rng.integers(1, 4)) + s + " " * int(self.rng.integers(0, 3))
            new[i] = cand
            detail.append(names[k])
        return new, detail

    def _t_amount_str(self, spec: TableSpec, col: str, old: np.ndarray):
        new = np.empty(old.size, dtype=object)
        detail = []
        for i, v in enumerate(old):
            f = float(v)
            if f == int(f) and self.rng.random() < 0.3:
                new[i], d = f"{f:,.0f}", "comma_0dp"
            else:
                new[i], d = f"{f:,.2f}", "comma_2dp"
            detail.append(d)
        return new, detail

    def _t_currency(self, spec: TableSpec, col: str, old: np.ndarray):
        new = np.empty(old.size, dtype=object)
        for i, v in enumerate(old):
            options = CURRENCY_VARIANTS.get(str(v), ["Ksh"])
            new[i] = options[int(self.rng.integers(0, len(options)))]
        return new, ["currency_variant"] * old.size

    def _t_nonpositive(self, spec: TableSpec, col: str, old: np.ndarray):
        zero = self.rng.random(old.size) < 0.3
        new = np.empty(old.size, dtype=object)
        for i, (v, z) in enumerate(zip(old, zero)):
            new[i] = 0.0 if z else -abs(float(v))
        return new, ["zero" if z else "negative" for z in zero]

    # ---- one cell-level defect pass ----------------------------------------------------
    def _cell_pass(self, defect_type, transform, spec, cols, data, touched, keys, refs, dt, label):
        rate = self.rates[defect_type]
        n = len(keys)
        if not cols or rate <= 0 or n == 0:
            return
        rows = self._pick(n, rate)
        if rows.size == 0:
            return
        which = self.rng.integers(0, len(cols), rows.size)
        for j, col in enumerate(cols):
            rr = rows[which == j]
            rr = rr[~touched[col][rr]]
            if rr.size == 0:
                continue
            old = data[col][rr].copy()
            new, detail = transform(spec, col, old)
            data[col][rr] = new
            touched[col][rr] = True
            self.log("messiness", dt, spec.name, label, keys[rr], refs[rr], col, defect_type, detail, old, new)

    # ---- public entry point ------------------------------------------------------------
    def inject(self, spec: TableSpec, df: pd.DataFrame, dt: str, label: str, is_last_day: bool) -> pd.DataFrame:
        order = list(df.columns)
        data = {c: df[c].to_numpy(dtype=object, copy=True) for c in order}
        keys = np.array([str(x) for x in data[spec.pk]], dtype=object)
        refs = np.array([str(x) for x in data["change_ts"]], dtype=object) if spec.cdc else np.full(len(keys), "", dtype=object)

        # 1) release rows held back from the previous day (out-of-order arrival)
        n_front = 0
        held = self.held.pop(spec.name, None)
        if held is not None and len(held[1]):
            hd, hk, hr, held_dt = held
            data = {c: np.concatenate([hd[c], data[c]]) for c in order}
            keys, refs = np.concatenate([hk, keys]), np.concatenate([hr, refs])
            n_front = len(hk)
            self.log(
                "messiness",
                dt,
                spec.name,
                label,
                hk,
                hr,
                "*",
                "out_of_order_update",
                [f"held_from={held_dt}"] * len(hk),
                [held_dt] * len(hk),
                [dt] * len(hk),
            )

        # 2) hold back some of today's update rows until tomorrow
        if spec.cdc and not is_last_day:
            cand = np.flatnonzero(data["op"] == "U")
            cand = cand[cand >= n_front]
            sel = cand[self.rng.random(cand.size) < self.rates["out_of_order_update"]]
            if sel.size:
                self.held[spec.name] = ({c: data[c][sel].copy() for c in order}, keys[sel].copy(), refs[sel].copy(), dt)
                keep = np.ones(len(keys), dtype=bool)
                keep[sel] = False
                data = {c: v[keep] for c, v in data.items()}
                keys, refs = keys[keep], refs[keep]

        # 3) test / dummy records (copies of real rows with marker values; exempt from cell defects)
        n_real = len(keys)
        k = int(self.rng.binomial(n_real, self.rates["test_record"])) if n_real else 0
        if k:
            src = self.rng.integers(0, n_real, k)
            ids = np.array([f"{spec.id_prefix}TEST{self._test_seq[spec.name] + i + 1:06d}" for i in range(k)], dtype=object)
            self._test_seq[spec.name] += k
            new = {c: data[c][src].copy() for c in order}
            new[spec.pk] = ids
            for col, val in spec.test_overrides.items():
                new[col] = np.full(k, val, dtype=object)
            if spec.cdc:
                new["op"] = np.full(k, "I", dtype=object)
            data = {c: np.concatenate([data[c], new[c]]) for c in order}
            keys = np.concatenate([keys, ids])
            refs = np.concatenate([refs, refs[src]])
            self.log("messiness", dt, spec.name, label, ids, refs[n_real:], "*", "test_record", ["dummy_row"] * k, [""] * k, ids)
        touched = {c: np.zeros(len(keys), dtype=bool) for c in order}
        if k:
            for c in order:
                touched[c][n_real:] = True

        # 4) cell-level defects (each cell is hit at most once)
        ts_like = tuple(spec.ts_cols) + tuple(spec.date_cols)
        passes = (
            ("null_required", self._t_null, spec.required),
            ("orphan_fk", self._t_orphan, spec.fks),
            ("mixed_date_format", self._t_datetime, ts_like),
            ("phone_variant", self._t_phone, spec.phone_cols),
            ("name_format", self._t_name, spec.name_cols),
            ("amount_string", self._t_amount_str, spec.amount_str_cols),
            ("currency_variant", self._t_currency, spec.currency_cols),
            ("nonpositive_amount", self._t_nonpositive, spec.nonpos_cols),
        )
        for defect_type, fn, cols in passes:
            self._cell_pass(defect_type, fn, spec, list(cols), data, touched, keys, refs, dt, label)

        # 5) exact duplicate rows (appended at the end of the file)
        dup = self._pick(len(keys), self.rates["duplicate_row"])
        if dup.size:
            data = {c: np.concatenate([data[c], data[c][dup]]) for c in order}
            self.log(
                "messiness", dt, spec.name, label, keys[dup], refs[dup], "*", "duplicate_row", ["exact_copy"] * dup.size, [""] * dup.size, keys[dup]
            )
        return pd.DataFrame(data, columns=order)


# =============================================================================
# Generator
# =============================================================================
@dataclass(frozen=True)
class Config:
    seed: int
    start_date: date
    days: int
    scale: float
    defect_rate: float
    out_dir: Path
    rate_overrides: Dict[str, float]
    schema_drift: bool = True
    drift_days: Tuple[int, int, int] = (0, 0, 0)  # (new column, rename, type change) day indexes


class Generator:
    def __init__(self, cfg: Config):
        self.cfg = cfg
        sim_ss, inj_ss = np.random.SeedSequence(cfg.seed).spawn(2)
        self.rng = np.random.default_rng(sim_ss)
        self.fake = Faker("en_GB")
        self.fake.seed_instance(cfg.seed)
        cfg.out_dir.mkdir(parents=True, exist_ok=True)
        rates = resolve_rates(cfg.defect_rate, cfg.rate_overrides)
        self.rates = rates
        self.injector = DefectInjector(np.random.default_rng(inj_ss), rates, cfg.out_dir / "defects_manifest.csv")
        self.row_counts: Counter = Counter()
        self.txn_seq = 0
        self.repay_seq = 0
        self.loan_seq = 0
        self.acct_seq = 0
        self.cust_seq = 0
        self.used_nids: set = set()
        self.pool: Deque[dict] = deque(maxlen=POOL_DAYS)
        self._build_reference_data()
        self._size_population()
        obj = "O"
        self.customers = ColumnStore({**{c: obj for c in CUSTOMER_COLS}, "primary_acct": "i8"})
        self.accounts = ColumnStore(
            {
                **{c: obj for c in ACCOUNT_COLS},
                "overdraft_limit": "f8",
                "account_tier": obj,
                "cust_row": "i8",
                "balance": "f8",
                "lam": "f8",
                "segf": "f8",
                "deleted": "?",
                "has_loan": "?",
            }
        )
        self.loans = ColumnStore(
            {
                **{c: obj for c in LOAN_COLS},
                "principal": "f8",
                "rate": "f8",
                "tenor_months": "i8",
                "days_past_due": "i8",
                "outstanding": "f8",
                "acct_row": "i8",
                "due_day": "i8",
                "disb_ord": "i8",
                "k_paid": "i8",
                "installment": "f8",
                "risk": "f8",
            }
        )
        self.base_produced = 0  # base (organic) txns generated so far
        self.day_index = 0

    # ---- reference data and sizing -----------------------------------------------------
    def _build_reference_data(self) -> None:
        rng = self.rng
        codes = [f"BR{i + 1:03d}" for i in range(len(BRANCH_DEFS))]
        self.branches_df = pd.DataFrame(
            {"branch_code": codes, "name": [f"{b[0]} Branch" for b in BRANCH_DEFS], "county": [b[1] for b in BRANCH_DEFS]}
        )
        self.branch_codes = np.array(codes, dtype=object)
        weights = np.array([b[2] for b in BRANCH_DEFS], dtype=float)
        self.branch_weights = weights / weights.sum()
        self.county_branches: Dict[str, Tuple[np.ndarray, np.ndarray]] = {}
        county_w: Dict[str, float] = {}
        for code, (_, county, w) in zip(codes, BRANCH_DEFS):
            county_w[county] = county_w.get(county, 0.0) + w
            self.county_branches.setdefault(county, ([], []))
            self.county_branches[county][0].append(code)
            self.county_branches[county][1].append(w)
        for county, (cs, ws) in list(self.county_branches.items()):
            ws = np.array(ws, dtype=float)
            self.county_branches[county] = (np.array(cs, dtype=object), ws / ws.sum())
        self.county_names = np.array(list(county_w.keys()), dtype=object)
        cw = np.array(list(county_w.values()), dtype=float)
        self.county_p = cw / cw.sum()
        persons = [f"{FIRST_NAMES[a]} {SURNAMES[b]}" for a, b in zip(rng.integers(0, len(FIRST_NAMES), 3000), rng.integers(0, len(SURNAMES), 3000))]
        self.pools = {
            "person": np.array(persons, dtype=object),
            "merchant": np.array(MERCHANTS, dtype=object),
            "biller": np.array(BILLERS, dtype=object),
            "airtime": np.array(["SAFARICOM AIRTIME", "AIRTEL AIRTIME"], dtype=object),
            "agent": np.array([f"AGENT {i:05d}" for i in range(500)], dtype=object),
            "atm": np.array([f"ATM{i:04d}" for i in range(200)], dtype=object),
            "counter": np.array(["OVER THE COUNTER"], dtype=object),
            "opening": np.array(["OPENING DEPOSIT"], dtype=object),
            "disbursement": np.array(["LOAN DISBURSEMENT"], dtype=object),
            "repayment": np.array(["LOAN REPAYMENT"], dtype=object),
        }

    def _size_population(self) -> None:
        c, s = self.cfg, self.cfg.scale
        growth = 1.0 + SIM.new_account_rate * c.days
        n_acc_final = max(20, round(SIM.accounts * s))
        n_cust_final = max(10, round(SIM.customers * s))
        self.n_acc0 = max(10, round(n_acc_final / growth))
        self.n_cust0 = max(5, round(n_cust_final * (1 - SIM.customer_growth_share)))
        self.n_cust0 = min(self.n_cust0, self.n_acc0)  # at least one account each
        self.cust_new_rate = max(0, n_cust_final - self.n_cust0) / c.days
        n_loans = max(5, round(SIM.loans * s))
        self.n_loan0 = max(2, round(n_loans * (1 - SIM.new_loan_share)))
        self.loan_new_rate = n_loans * SIM.new_loan_share / c.days
        self.target_txns = SIM.txns_total * s * c.days / 365.0

    # ---- random primitives -------------------------------------------------------------
    def _intraday(self, n: int) -> np.ndarray:
        if n == 0:
            return np.empty(0, dtype="int64")
        hours = self.rng.choice(24, size=n, p=HOUR_P)
        return hours * 3600 + self.rng.integers(0, 3600, size=n)

    def _counterparties(self, types: np.ndarray) -> np.ndarray:
        out = np.empty(len(types), dtype=object)
        for t in np.unique(types):
            m = types == t
            pool = self.pools[TYPE_POOL[t]]
            out[m] = pool[self.rng.integers(0, len(pool), int(m.sum()))]
        return out

    def _references(self, channels: np.ndarray) -> np.ndarray:
        out = np.empty(len(channels), dtype=object)
        codes = random_codes(self.rng, len(channels), 10)
        for ch in np.unique(channels):
            m = np.flatnonzero(channels == ch)
            pre = REF_PREFIX[ch]
            out[m] = [pre + c[: 10 - len(pre)] for c in codes[m]]
        return out

    def _unique_national_ids(self, n: int) -> List[str]:
        out: List[str] = []
        while len(out) < n:
            for v in self.rng.integers(10_000_000, 40_000_000, size=n - len(out)):
                s = f"{int(v):08d}"
                if s not in self.used_nids:
                    self.used_nids.add(s)
                    out.append(s)
        return out

    def _to_account_ccy(self, kes: np.ndarray, acct_rows: np.ndarray) -> np.ndarray:
        usd = self.accounts["currency"][acct_rows] == "USD"
        return np.where(usd, np.maximum(np.round(kes / KES_PER_USD, 2), 1.0), kes)

    # ---- entity creation ---------------------------------------------------------------
    def _create_customers(self, n: int, created_ts: np.ndarray, snapshot: bool) -> np.ndarray:
        rng = self.rng
        seg = rng.choice(SEGMENTS, size=n, p=SEG_P)
        first = rng.choice(FIRST_NAMES, size=n)
        last = rng.choice(SURNAMES, size=n)
        names, emails = [], []
        nids = self._unique_national_ids(n)
        dob = np.empty(n, dtype=object)
        ref_ord = self.cfg.start_date.toordinal()
        for i in range(n):
            ln = str(last[i])
            slug = ln.lower().replace("'", "")
            domain = self.fake.free_email_domain()
            if seg[i] == "corporate":
                biz = str(rng.choice(BUSINESS_WORDS))
                names.append(f"{ln} {biz} {self.fake.company_suffix()}")
                emails.append(f"info@{slug}{biz.lower()}{EMAIL_BIZ_DOMAIN}")
                nids[i] = f"CPR/{int(rng.integers(1995, 2024))}/{int(rng.integers(0, 1_000_000)):06d}"
                dob[i] = date.fromordinal(ref_ord - int(rng.integers(365, 365 * 30))).isoformat()
            else:
                if seg[i] == "SME" and rng.random() < 0.4:
                    names.append(f"{ln} {rng.choice(BUSINESS_WORDS)}")
                else:
                    names.append(f"{first[i]} {ln}")
                emails.append(f"{str(first[i]).lower()}.{slug}{int(rng.integers(1, 99))}@{domain}")
                age = float(np.clip(rng.normal(36, 11), 18, 80))
                dob[i] = date.fromordinal(ref_ord - int(age * 365.25)).isoformat()
        lead = rng.choice(["7", "1"], size=n, p=[0.88, 0.12])
        phones = [f"+254{d}{int(x):08d}" for d, x in zip(lead, rng.integers(0, 100_000_000, n))]
        kyc_p = [0.86, 0.08, 0.03, 0.03] if snapshot else [0.50, 0.45, 0.03, 0.02]
        kyc = rng.choice(["verified", "pending", "rejected", "expired"], size=n, p=kyc_p)
        county = rng.choice(self.county_names, size=n, p=self.county_p)
        ids = [f"C{self.cust_seq + i + 1:07d}" for i in range(n)]
        self.cust_seq += n
        return self.customers.append(
            {
                "customer_id": ids,
                "full_name": names,
                "national_id": nids,
                "dob": dob,
                "phone": phones,
                "email": emails,
                "county": county,
                "segment": seg,
                "kyc_status": kyc,
                "created_at": created_ts,
                "primary_acct": np.full(n, -1),
            }
        )

    def _create_accounts(self, cust_rows: np.ndarray, is_primary: np.ndarray, opened_at: np.ndarray, snapshot: bool) -> np.ndarray:
        rng, cu = self.rng, self.customers
        n = len(cust_rows)
        seg = cu["segment"][cust_rows]
        product = np.empty(n, dtype=object)
        for s in SEGMENTS:
            m = seg == s
            if m.any():
                product[m] = rng.choice(PRODUCTS, size=int(m.sum()), p=PRODUCT_P[s])
        product[is_primary & (product == "fixed_deposit")] = "savings"
        usd = (~is_primary) & (rng.random(n) < SIM.usd_share_non_primary)
        currency = np.where(usd, "USD", "KES").astype(object)
        county = cu["county"][cust_rows]
        branch = np.empty(n, dtype=object)
        for cty in np.unique(county):
            m = county == cty
            codes, p = self.county_branches[cty]
            branch[m] = rng.choice(codes, size=int(m.sum()), p=p)
        if snapshot:
            status = rng.choice(["active", "dormant", "frozen", "closed"], size=n, p=[0.90, 0.06, 0.015, 0.025]).astype(object)
        else:
            status = np.full(n, "active", dtype=object)
        overdraft = np.zeros(n)
        for s in SEGMENTS:
            m = (seg == s) & (product == "current") & (currency == "KES")
            if m.any():
                overdraft[m] = rng.choice(OVERDRAFT_LEVELS[s], size=int(m.sum()))
        balance = np.empty(n)
        tier = np.empty(n, dtype=object)
        segf = np.empty(n)
        for s in SEGMENTS:
            m = seg == s
            k = int(m.sum())
            if k:
                balance[m] = rng.lognormal(BALANCE_LOG_MEAN[s], 1.0, k)
                tier[m] = rng.choice(["basic", "standard", "premium"], size=k, p=TIER_P[s])
                segf[m] = SEG_AMOUNT_FACTOR[s]
        balance *= np.where(product == "fixed_deposit", 5.0, 1.0)
        balance = np.round(np.where(usd, balance / KES_PER_USD, balance), 2)
        balance[status == "closed"] = 0.0
        raw = rng.gamma(0.8, 1.25, n) * np.array([PRODUCT_ACTIVITY[p] for p in product])
        ids = [f"AC{100_000_001 + self.acct_seq + i:09d}" for i in range(n)]
        self.acct_seq += n
        idx = self.accounts.append(
            {
                "account_id": ids,
                "customer_id": cu["customer_id"][cust_rows],
                "product_type": product,
                "branch_code": branch,
                "currency": currency,
                "status": status,
                "overdraft_limit": overdraft,
                "opened_at": opened_at,
                "account_tier": tier,
                "cust_row": cust_rows,
                "balance": balance,
                "lam": raw,
                "segf": segf,
                "deleted": np.zeros(n, dtype=bool),
                "has_loan": np.zeros(n, dtype=bool),
            }
        )
        pr = np.flatnonzero(is_primary)
        cu["primary_acct"][cust_rows[pr]] = idx[pr]
        return idx

    def _loan_candidates(self) -> np.ndarray:
        prim = self.customers["primary_acct"]
        cand = np.flatnonzero(prim >= 0)
        pa = prim[cand]
        ok = (self.accounts["status"][pa] == "active") & ~self.accounts["deleted"][pa]
        return cand[ok]

    def _create_loans(self, n: int, d: date, snapshot: bool) -> np.ndarray:
        rng, cu, ac = self.rng, self.customers, self.accounts
        cand = self._loan_candidates()
        if n == 0 or cand.size == 0:
            return np.empty(0, dtype=int)
        seg_c = cu["segment"][cand]
        w = np.where(seg_c == "corporate", 8.0, np.where(seg_c == "SME", 4.0, 1.0))
        cust_rows = rng.choice(cand, size=n, p=w / w.sum())
        seg = cu["segment"][cust_rows]
        principal, rate = np.empty(n), np.empty(n)
        for s, (mu, sg, lo, hi, r_lo, r_hi) in LOAN_PARAMS.items():
            m = seg == s
            k = int(m.sum())
            if k:
                principal[m] = np.clip(rng.lognormal(mu, sg, k), lo, hi)
                rate[m] = rng.uniform(r_lo, r_hi, k)
        principal = np.round(principal / 1000.0) * 1000.0
        rate = np.round(rate, 2)
        tenor = rng.choice(TENORS, size=n, p=TENOR_P)
        sec = self._intraday(n)
        if snapshot:
            elapsed = np.floor(rng.random(n) * tenor * 0.6).astype(int)  # months since disbursal
            matured = rng.random(n) < 0.04  # a few already repaid
            elapsed[matured] = tenor[matured]
            days_ago = np.maximum(20, elapsed * 30 + rng.integers(0, 30, n))
            base_days = np.datetime64(d, "D") - days_ago.astype("timedelta64[D]")
            disb_ts = format_ts(base_days, sec)
            disb_dom = pd.DatetimeIndex(base_days).day.to_numpy()
            disb_ord = d.toordinal() - days_ago
        else:
            disb_ts = format_ts(d, sec)
            disb_dom = np.full(n, d.day)
            disb_ord = np.full(n, d.toordinal())
        status = np.full(n, "current", dtype=object)
        dpd = np.zeros(n, dtype="int64")
        k_paid = np.zeros(n, dtype="int64")
        outstanding = principal.copy()
        installment = np.array([annuity_installment(p, r, int(t)) for p, r, t in zip(principal, rate, tenor)])
        if snapshot:
            months = days_ago // 30
            u = rng.random(n)
            for i in range(n):
                m = int(months[i])
                if m >= tenor[i]:
                    status[i], k_paid[i], outstanding[i] = "closed", tenor[i], 0.0
                    continue
                if u[i] < 0.80:
                    k = m
                elif u[i] < 0.93:
                    status[i], dpd[i] = "overdue", int(rng.integers(1, 91))
                    k = max(0, m - -(-int(dpd[i]) // 30))
                else:
                    status[i], dpd[i] = "default", int(rng.integers(91, 361))
                    k = max(0, m - -(-int(dpd[i]) // 30))
                k_paid[i] = k
                outstanding[i] = balance_after_k(principal[i], rate[i], int(tenor[i]), k)
        acct_rows = cu["primary_acct"][cust_rows]
        ids = [f"LN{10_000_001 + self.loan_seq + i:08d}" for i in range(n)]
        self.loan_seq += n
        idx = self.loans.append(
            {
                "loan_id": ids,
                "customer_id": cu["customer_id"][cust_rows],
                "account_id": ac["account_id"][acct_rows],
                "principal": principal,
                "rate": rate,
                "tenor_months": tenor,
                "disbursed_at": disb_ts,
                "status": status,
                "days_past_due": dpd,
                "outstanding": np.round(outstanding, 2),
                "acct_row": acct_rows,
                "due_day": np.minimum(disb_dom, 28),
                "disb_ord": disb_ord,
                "k_paid": k_paid,
                "installment": np.round(installment, 2),
                "risk": rng.gamma(4.0, 0.25, n),
            }
        )
        open_mask = status != "closed"
        ac["has_loan"][acct_rows[open_mask]] = True
        return idx

    # ---- transaction parts -------------------------------------------------------------
    def _part(
        self,
        acct_row,
        amount,
        direction,
        channel,
        txn_type,
        counterparty,
        reference,
        tag: str = "",
        ev_sec=None,
        post_sec=None,
        lag=None,
        force_fail=None,
    ) -> dict:
        n = len(acct_row)
        sec = self._intraday(n) if post_sec is None else post_sec
        return {
            "acct_row": np.asarray(acct_row, dtype="int64"),
            "amount": np.asarray(amount, dtype=float),
            "direction": np.asarray(direction, dtype=object),
            "channel": np.asarray(channel, dtype=object),
            "txn_type": np.asarray(txn_type, dtype=object),
            "counterparty": np.asarray(counterparty, dtype=object),
            "reference": np.asarray(reference, dtype=object),
            "force_fail": np.zeros(n, dtype=bool) if force_fail is None else force_fail,
            "post_sec": sec,
            "ev_sec": sec if ev_sec is None else ev_sec,
            "lag_days": np.zeros(n, dtype="int64") if lag is None else lag,
            "tag": np.full(n, tag, dtype=object) if isinstance(tag, str) else tag,
        }

    def _base_txns(self) -> dict:
        rng, st = self.rng, self.accounts
        active = np.flatnonzero((st["status"] == "active") & ~st["deleted"])
        # Volume controller: rescale per-account rates each day so the run lands on the target
        # total regardless of how many accounts have gone dormant/closed.
        expected = float(st["lam"][active].sum())
        days_left = self.cfg.days - self.day_index
        remaining = max(0.0, self.target_txns * 0.96 - self.base_produced)
        scale = float(np.clip(remaining / days_left / expected, 0.2, 5.0)) if expected > 0 else 0.0
        counts = rng.poisson(st["lam"][active] * scale)
        self.base_produced += int(counts.sum())
        rows = np.repeat(active, counts)
        n = rows.size
        if n == 0:
            return self._part([], [], [], [], [], [], [])
        chan_i = rng.choice(len(CHANNELS), size=n, p=CHANNEL_P)
        is_dr = rng.random(n) < DR_SHARE[chan_i]
        direction = np.where(is_dr, "DR", "CR").astype(object)
        channel = np.array(CHANNELS, dtype=object)[chan_i]
        ttype = np.empty(n, dtype=object)
        for ci, ch in enumerate(CHANNELS):
            for flag, dname in ((True, "DR"), (False, "CR")):
                m = (chan_i == ci) & (is_dr == flag)
                k = int(m.sum())
                if k:
                    names, p = TXN_TYPES[(ch, dname)]
                    ttype[m] = rng.choice(names, size=k, p=p)
        amt = np.empty(n)
        segf = st["segf"][rows]
        for ci, ch in enumerate(CHANNELS):
            m = chan_i == ci
            k = int(m.sum())
            if not k:
                continue
            if ch == "ATM":
                a = 500.0 * np.minimum(rng.geometric(0.25, k), 80)
            else:
                mu, sg = AMOUNT_LOGNORMAL[ch]
                a = rng.lognormal(mu, sg, k) * segf[m]
            a = np.minimum(a, CHANNEL_CAP[ci])
            amt[m] = np.round(a, 0) if ch in WHOLE_SHILLING_CHANNELS else np.round(a, 2)
        amt = self._to_account_ccy(np.maximum(amt, 10.0), rows)
        late = rng.random(n) < SIM.late_rate
        lag = np.where(late, rng.integers(1, 4, n), 0)
        ev_sec = self._intraday(n)
        branch_m = channel == "branch"
        ev_sec[branch_m] = rng.integers(8 * 3600, 16 * 3600, int(branch_m.sum()))
        post_sec = np.where(late, self._intraday(n), ev_sec)
        tag = np.where(late, "late_arrival", "").astype(object)
        return self._part(
            rows,
            amt,
            direction,
            channel,
            ttype,
            self._counterparties(ttype),
            self._references(channel),
            tag=tag,
            ev_sec=ev_sec,
            post_sec=post_sec,
            lag=lag,
            force_fail=rng.random(n) < SIM.fail_rate,
        )

    def _reversals_and_corrections(self) -> List[dict]:
        rng, parts = self.rng, []
        for kind, rate in (("reversal", SIM.reversal_rate), ("correction", SIM.correction_rate)):
            for entry in self.pool:
                avail = np.flatnonzero(~entry["used"])
                k = int(rng.binomial(avail.size, rate / POOL_DAYS)) if avail.size else 0
                if not k:
                    continue
                pick = rng.choice(avail, size=k, replace=False)
                entry["used"][pick] = True
                orig_dir = entry["direction"][pick]
                flipped = np.where(orig_dir == "DR", "CR", "DR").astype(object)
                if kind == "reversal":
                    amount, direction = entry["amount"][pick], flipped
                else:
                    amount = np.maximum(np.round(entry["amount"][pick] * rng.uniform(0.02, 0.4, k), 2), 1.0)
                    direction = np.where(rng.random(k) < 0.5, orig_dir, flipped).astype(object)
                parts.append(
                    self._part(
                        entry["acct_row"][pick],
                        amount,
                        direction,
                        entry["channel"][pick],
                        np.full(k, kind.upper(), dtype=object),
                        entry["counterparty"][pick],
                        entry["reference"][pick],
                        tag=kind,
                    )
                )
        return parts

    def _post_transactions(self, d: date, parts: List[dict]):
        """Order by account/posting time, compute balances, enforce overdraft, assign ids."""
        keys = list(parts[0].keys())
        cat = {k: np.concatenate([p[k] for p in parts]) for k in keys}
        n = len(cat["acct_row"])
        if n == 0:
            return None, None
        st = self.accounts
        order = np.lexsort((cat["post_sec"], cat["acct_row"]))
        cat = {k: v[order] for k, v in cat.items()}
        acct, amt = cat["acct_row"], cat["amount"]
        is_cr = cat["direction"] == "CR"
        start = st["balance"][acct]
        od = st["overdraft_limit"][acct]
        signed = np.where(cat["force_fail"], 0.0, np.where(is_cr, amt, -amt))
        bal = running_balance(acct, signed, start)
        violated = (~is_cr) & (~cat["force_fail"]) & (bal < -od - 1e-9)
        failed = cat["force_fail"] | violated
        if violated.any():  # drop declined debits, recompute balances
            signed = np.where(failed, 0.0, np.where(is_cr, amt, -amt))
            bal = running_balance(acct, signed, start)
        np.add.at(st["balance"], acct, signed)
        touched_accts = np.unique(acct)
        st["balance"][touched_accts] = np.round(st["balance"][touched_accts], 2)
        fo = np.argsort(cat["post_sec"], kind="stable")
        cat = {k: v[fo] for k, v in cat.items()}
        failed, bal, acct = failed[fo], bal[fo], acct[fo]
        seq = self.txn_seq + np.arange(1, n + 1)
        self.txn_seq += n
        ids = np.char.add("TXN", np.char.zfill(seq.astype(str), 10)).astype(object)
        ts = format_ts(d, cat["ev_sec"] - cat["lag_days"] * 86400)
        df = pd.DataFrame(
            {
                "txn_id": ids,
                "account_id": st["account_id"][acct],
                "txn_ts": ts,
                "amount": cat["amount"],
                "currency": st["currency"][acct],
                "direction": cat["direction"],
                "channel": cat["channel"],
                "txn_type": cat["txn_type"],
                "counterparty": cat["counterparty"],
                "balance_after": np.round(bal, 2),
                "status": np.where(failed, "failed", "posted").astype(object),
                "reference": cat["reference"],
            }
        )
        eligible = (cat["tag"] == "") & ~failed & ~np.isin(cat["txn_type"], NON_REVERSIBLE_TYPES)
        sel = np.flatnonzero(eligible)
        self.pool.append(
            {
                "acct_row": acct[sel],
                "amount": cat["amount"][sel],
                "direction": cat["direction"][sel],
                "channel": cat["channel"][sel],
                "counterparty": cat["counterparty"][sel],
                "reference": cat["reference"][sel],
                "used": np.zeros(sel.size, dtype=bool),
            }
        )
        return df, {"tag": cat["tag"], "lag": cat["lag_days"]}

    # ---- daily account / customer changes -----------------------------------------------
    def _daily_account_changes(self, d: date, cust_frames: list, acct_frames: list) -> None:
        rng, st, cu = self.rng, self.accounts, self.customers
        live = np.flatnonzero(~st["deleted"] & (st["status"] != "closed"))
        k = min(int(rng.binomial(int((~st["deleted"]).sum()), SIM.update_rate)), live.size)
        if k:
            idx = np.sort(rng.choice(live, size=k, replace=False))
            u = rng.random(k)
            kind = np.where(u < 0.35, "status", np.where(u < 0.60, "overdraft", np.where(u < 0.85, "branch", "phone")))
            od_ok = (st["product_type"][idx] == "current") & (st["currency"][idx] == "KES")
            kind[(kind == "overdraft") & ~od_ok] = "branch"
            changed = []
            for a in idx[kind == "status"]:
                choices, p = STATUS_TRANSITIONS[st.cols["status"][a]]
                st.cols["status"][a] = choices[int(rng.choice(len(choices), p=p))]
                changed.append(a)
            for a in idx[kind == "overdraft"]:
                seg = cu.cols["segment"][st.cols["cust_row"][a]]
                options = [x for x in OVERDRAFT_LEVELS[seg] if x != st.cols["overdraft_limit"][a]]
                st.cols["overdraft_limit"][a] = float(rng.choice(options))
                changed.append(a)
            for a in idx[kind == "branch"]:
                options = self.branch_codes[self.branch_codes != st.cols["branch_code"][a]]
                st.cols["branch_code"][a] = options[int(rng.integers(0, len(options)))]
                changed.append(a)
            phone_custs = np.unique(st["cust_row"][idx[kind == "phone"]])
            if changed:
                ch = np.array(sorted(changed))
                acct_frames.append(cdc_frame(st, list(SPECS["accounts"].columns) + ["account_tier"], ch, "U", format_ts(d, self._intraday(ch.size))))
            if phone_custs.size:
                lead = rng.choice(["7", "1"], size=phone_custs.size, p=[0.88, 0.12])
                for c, l, x in zip(phone_custs, lead, rng.integers(0, 100_000_000, phone_custs.size)):
                    cu.cols["phone"][c] = f"+254{l}{int(x):08d}"
                cust_frames.append(cdc_frame(cu, CUSTOMER_COLS, phone_custs, "U", format_ts(d, self._intraday(phone_custs.size))))
        cand = np.flatnonzero((st["status"] == "closed") & ~st["deleted"] & ~st["has_loan"])
        n_del = min(int(rng.poisson(SIM.delete_rate * live.size)), cand.size)
        if n_del:
            gone = np.sort(rng.choice(cand, size=n_del, replace=False))
            acct_frames.append(cdc_frame(st, list(SPECS["accounts"].columns) + ["account_tier"], gone, "D", format_ts(d, self._intraday(n_del))))
            st["deleted"][gone] = True

    # ---- new customers / accounts ----------------------------------------------------------
    def _daily_new_accounts(self, d: date, cust_frames: list, acct_frames: list, txn_parts: list) -> None:
        rng, st, cu = self.rng, self.accounts, self.customers
        n_c = int(rng.poisson(self.cust_new_rate))
        n_live = int((~st["deleted"]).sum())
        n_a = max(int(rng.poisson(SIM.new_account_rate * n_live)), n_c)
        if n_a == 0:
            return
        n_before = cu.n
        sec_new = np.sort(self._intraday(n_c))
        new_idx = np.empty(0, dtype=int)
        if n_c:
            new_idx = self._create_customers(n_c, format_ts(d, sec_new), snapshot=False)
            cust_frames.append(cdc_frame(cu, CUSTOMER_COLS, new_idx, "I", format_ts(d, sec_new)))
        n_ex = n_a - n_c
        ex_rows = rng.integers(0, n_before, n_ex) if n_ex else np.empty(0, dtype=int)
        cust_rows = np.concatenate([new_idx, ex_rows]).astype(int)
        is_primary = cu["primary_acct"][cust_rows] < 0
        sec_open = np.concatenate([sec_new, self._intraday(n_ex)]).astype("int64")
        idx = self._create_accounts(cust_rows, is_primary, format_ts(d, sec_open), snapshot=False)
        acct_frames.append(cdc_frame(st, list(SPECS["accounts"].columns) + ["account_tier"], idx, "I", format_ts(d, sec_open)))
        kes = np.maximum(np.round(rng.lognormal(8.5, 1.0, n_a) * st["segf"][idx], 0), 100.0)
        chan = np.where(rng.random(n_a) < 0.6, "branch", "mobile app").astype(object)
        txn_parts.append(
            self._part(
                idx,
                self._to_account_ccy(kes, idx),
                np.full(n_a, "CR", dtype=object),
                chan,
                np.full(n_a, "OPENING_DEPOSIT", dtype=object),
                self._counterparties(np.full(n_a, "OPENING_DEPOSIT", dtype=object)),
                np.char.add("OPN", random_codes(rng, n_a, 7).astype(str)).astype(object),
                post_sec=np.minimum(sec_open + rng.integers(60, 3600, n_a), 86_399),
            )
        )

    # ---- loans ---------------------------------------------------------------------------
    def _daily_loans(self, d: date, loan_frames: list, txn_parts: list) -> Optional[pd.DataFrame]:
        rng, ls, st = self.rng, self.loans, self.accounts
        S = ls.cols
        changed: List[int] = []
        rp_loan: List[int] = []
        rp_amt: List[float] = []
        rp_sec: List[int] = []
        pay_txn: List[Tuple[int, float, int]] = []

        def record(li: int, amount: float) -> None:
            sec = int(self._intraday(1)[0])
            rp_loan.append(li)
            rp_amt.append(round(amount, 2))
            rp_sec.append(sec)
            a = int(S["acct_row"][li])
            if st.cols["status"][a] == "active" and not st.cols["deleted"][a]:
                pay_txn.append((a, round(amount, 2), sec))

        def close(li: int) -> None:
            S["status"][li], S["outstanding"][li], S["days_past_due"][li] = "closed", 0.0, 0

        due = np.flatnonzero((ls["status"] != "closed") & (ls["due_day"] == d.day) & (ls["disb_ord"] < d.toordinal()))
        draws = rng.random((due.size, 3))
        for pos, li in enumerate(due):
            u1, u2, _ = draws[pos]
            status, A, r, out = S["status"][li], S["installment"][li], S["rate"][li], S["outstanding"][li]
            tenor = S["tenor_months"][li]
            if status == "current":
                if S["k_paid"][li] >= tenor or out <= 0.005:
                    close(li)
                elif u1 < SIM.p_miss * S["risk"][li]:
                    S["status"][li], S["days_past_due"][li] = "overdue", 1 + int(u2 * 30)
                else:
                    pay = min(A, out * (1 + r / 1200.0))
                    new_out = apply_payment(out, pay, r)
                    S["k_paid"][li] += 1
                    S["outstanding"][li] = new_out
                    record(li, pay)
                    if new_out <= 0.005 or S["k_paid"][li] >= tenor:
                        close(li)
                    else:
                        continue  # plain on-time payment: no CDC row
                changed.append(li)
            elif status == "overdue":
                dpd = int(S["days_past_due"][li])
                if u1 < SIM.p_cure:
                    missed, total = max(1, -(-dpd // 30)), 0.0
                    for _ in range(missed):
                        pay = min(A, out * (1 + r / 1200.0))
                        out = apply_payment(out, pay, r)
                        total += pay
                        S["k_paid"][li] += 1
                        if out <= 0.005:
                            break
                    S["outstanding"][li], S["days_past_due"][li], S["status"][li] = out, 0, "current"
                    record(li, total)
                    if out <= 0.005 or S["k_paid"][li] >= tenor:
                        close(li)
                else:
                    dpd += 30
                    S["days_past_due"][li] = dpd
                    if dpd > 90:
                        S["status"][li] = "default"
                changed.append(li)
            else:  # default
                if u1 < SIM.p_recover:
                    pay = round(out * (0.03 + 0.17 * u2), 2)
                    S["outstanding"][li] = max(0.0, out - pay)
                    record(li, pay)
                    if S["outstanding"][li] <= 0.005:
                        close(li)
                        changed.append(li)
                elif u1 < SIM.p_recover + SIM.p_writeoff:
                    close(li)
                    changed.append(li)
        # ad-hoc part-payments (top-ups between instalments); reduce principal, never change status
        elig = np.flatnonzero(((ls["status"] == "current") | (ls["status"] == "overdue")) & (ls["disb_ord"] < d.toordinal()))
        pick = elig[rng.random(elig.size) < SIM.p_extra_payment]
        shares = rng.uniform(0.2, 1.0, pick.size)
        for li, share in zip(pick, shares):
            pay = round(min(S["installment"][li] * share, S["outstanding"][li]), 2)
            if pay <= 0:
                continue
            S["outstanding"][li] = apply_payment(S["outstanding"][li], pay, S["rate"][li])
            record(li, pay)
            if S["outstanding"][li] <= 0.005:
                close(li)
                changed.append(li)
        if changed:
            ch = np.array(sorted(set(changed)))
            S["outstanding"][ch] = np.round(S["outstanding"][ch], 2)
            loan_frames.append(cdc_frame(ls, LOAN_COLS, ch, "U", format_ts(d, self._intraday(ch.size))))
        new_idx = self._create_loans(int(rng.poisson(self.loan_new_rate)), d, snapshot=False)
        if new_idx.size:
            loan_frames.append(cdc_frame(ls, LOAN_COLS, new_idx, "I", ls["disbursed_at"][new_idx]))
            m = new_idx.size
            chan = np.where(rng.random(m) < 0.6, "branch", "mobile app").astype(object)
            txn_parts.append(
                self._part(
                    ls["acct_row"][new_idx],
                    ls["principal"][new_idx],
                    np.full(m, "CR", dtype=object),
                    chan,
                    np.full(m, "LOAN_DISBURSEMENT", dtype=object),
                    self._counterparties(np.full(m, "LOAN_DISBURSEMENT", dtype=object)),
                    np.char.add("LN", random_codes(rng, m, 8).astype(str)).astype(object),
                )
            )
        if pay_txn:
            a_rows = np.array([p[0] for p in pay_txn])
            m = a_rows.size
            chan = rng.choice(["M-Pesa", "mobile app", "branch"], size=m, p=[0.5, 0.35, 0.15]).astype(object)
            txn_parts.append(
                self._part(
                    a_rows,
                    [p[1] for p in pay_txn],
                    np.full(m, "DR", dtype=object),
                    chan,
                    np.full(m, "LOAN_REPAYMENT", dtype=object),
                    self._counterparties(np.full(m, "LOAN_REPAYMENT", dtype=object)),
                    np.char.add("LN", random_codes(rng, m, 8).astype(str)).astype(object),
                    post_sec=np.array([p[2] for p in pay_txn], dtype="int64"),
                )
            )
        if not rp_loan:
            return None
        n = len(rp_loan)
        seq = self.repay_seq + np.arange(1, n + 1)
        self.repay_seq += n
        return pd.DataFrame(
            {
                "repayment_id": [f"RP{x:09d}" for x in seq],
                "loan_id": ls["loan_id"][np.array(rp_loan)],
                "paid_at": format_ts(d, np.array(rp_sec)),
                "amount": np.array(rp_amt),
            }
        )

    # ---- day-0 snapshot --------------------------------------------------------------------
    def _snapshot(self, d: date, cust_frames: list, acct_frames: list, loan_frames: list) -> None:
        rng = self.rng
        n_c, n_a = self.n_cust0, self.n_acc0
        created_ago = rng.integers(30, 3650, n_c)
        created_days = np.datetime64(d, "D") - created_ago.astype("timedelta64[D]")
        cidx = self._create_customers(n_c, format_ts(created_days, self._intraday(n_c)), snapshot=True)
        extra = max(0, n_a - n_c)
        seg = self.customers["segment"]
        w = np.where(seg == "corporate", 3.0, np.where(seg == "SME", 2.0, 1.0))
        extra_rows = rng.choice(n_c, size=extra, p=w / w.sum()) if extra else np.empty(0, dtype=int)
        cust_rows = np.concatenate([np.arange(n_c), extra_rows]).astype(int)
        is_primary = np.concatenate([np.ones(n_c, dtype=bool), np.zeros(extra, dtype=bool)])
        ago = np.where(
            is_primary,
            created_ago[cust_rows] - rng.integers(0, 3, cust_rows.size),
            np.floor(created_ago[cust_rows] * rng.random(cust_rows.size)).astype(int),
        )
        ago = np.maximum(ago, 1)
        opened = format_ts(np.datetime64(d, "D") - ago.astype("timedelta64[D]"), self._intraday(cust_rows.size))
        aidx = self._create_accounts(cust_rows, is_primary, opened, snapshot=True)

        def snap_ts(n):
            return format_ts(d, np.zeros(n, dtype="int64"))

        cust_frames.append(cdc_frame(self.customers, CUSTOMER_COLS, cidx, "I", snap_ts(cidx.size)))
        acct_frames.append(cdc_frame(self.accounts, list(SPECS["accounts"].columns) + ["account_tier"], aidx, "I", snap_ts(aidx.size)))
        lidx = self._create_loans(self.n_loan0, d, snapshot=True)
        if lidx.size:
            loan_frames.append(cdc_frame(self.loans, LOAN_COLS, lidx, "I", snap_ts(lidx.size)))

    # ---- emit ------------------------------------------------------------------------------
    def _label(self, spec: TableSpec, d: date) -> str:
        return f"{spec.name}/dt={d.isoformat()}/{spec.name}_{d:%Y%m%d}.{spec.ext}"

    def _empty_frame(self, spec: TableSpec) -> pd.DataFrame:
        return pd.DataFrame({c: pd.Series([], dtype=object) for c in spec.frame_columns})

    def _log_drift(self, spec, label, d, field_name, drift_type, original, injected) -> None:
        self.injector.log(
            "schema_drift",
            d.isoformat(),
            spec.name,
            label,
            ["*"],
            [""],
            field_name,
            drift_type,
            [f"applies_from_day_index_{self._drift_day(drift_type)}"],
            [original],
            [injected],
        )

    def _drift_day(self, drift_type: str) -> int:
        a, b, c = self.cfg.drift_days
        return {"new_column": a, "renamed_column": b, "type_change": c}[drift_type]

    def _apply_drift(self, spec: TableSpec, df: pd.DataFrame, d: date, i: int, label: str) -> pd.DataFrame:
        cfg = self.cfg
        if spec.name == "accounts":
            if cfg.schema_drift and i >= cfg.drift_days[0]:
                self._log_drift(spec, label, d, "account_tier", "new_column", "<absent>", "account_tier")
            else:
                df = df.drop(columns=["account_tier"])
        elif spec.name == "loans" and cfg.schema_drift and i >= cfg.drift_days[1]:
            df = df.rename(columns={"rate": "interest_rate"})
            self._log_drift(spec, label, d, "rate", "renamed_column", "rate", "interest_rate")
        elif spec.name == "transactions" and cfg.schema_drift and i >= cfg.drift_days[2]:
            df = df.copy()
            df["balance_after"] = [None if (v is None or v != v) else f"{float(v):.2f}" for v in df["balance_after"]]
            self._log_drift(spec, label, d, "balance_after", "type_change", "number", "string")
        return df

    def _emit(self, spec: TableSpec, df: Optional[pd.DataFrame], d: date, i: int, sort_cdc: bool = True) -> pd.DataFrame:
        if df is None or df.empty:
            df = self._empty_frame(spec)
        elif spec.cdc and sort_cdc:
            df = df.sort_values("change_ts", kind="stable").reset_index(drop=True)
        label = self._label(spec, d)
        df = self.injector.inject(spec, df, d.isoformat(), label, i == self.cfg.days - 1)
        if spec.fmt == "parquet":
            df = cast_parquet(spec, df)
        df = self._apply_drift(spec, df, d, i, label)
        write_frame(spec, df, self.cfg.out_dir / label)
        self.row_counts[spec.name] += len(df)
        return df

    @staticmethod
    def _concat(frames: List[pd.DataFrame]) -> Optional[pd.DataFrame]:
        return pd.concat(frames, ignore_index=True) if frames else None

    # ---- main loop -------------------------------------------------------------------------
    def simulate_day(self, i: int) -> None:
        d = self.cfg.start_date + timedelta(days=i)
        self.day_index = i
        cust_f: List[pd.DataFrame] = []
        acct_f: List[pd.DataFrame] = []
        loan_f: List[pd.DataFrame] = []
        parts: List[dict] = []
        if i == 0:
            self._snapshot(d, cust_f, acct_f, loan_f)
            self._emit(SPECS["branches"], self.branches_df, d, i)
        else:
            self._daily_account_changes(d, cust_f, acct_f)
        parts.append(self._base_txns())
        if i > 0:
            self._daily_new_accounts(d, cust_f, acct_f, parts)
        repay_df = self._daily_loans(d, loan_f, parts)
        parts.extend(self._reversals_and_corrections())
        txn_df, meta = self._post_transactions(d, parts)
        self._emit(SPECS["customers"], self._concat(cust_f), d, i)
        self._emit(SPECS["accounts"], self._concat(acct_f), d, i)
        self._emit(SPECS["loans"], self._concat(loan_f), d, i)
        self._emit(SPECS["loan_repayments"], repay_df, d, i)
        if txn_df is not None:
            self._log_simulated_events(txn_df, meta, d)
        self._emit(SPECS["transactions"], txn_df, d, i)
        self.injector.flush()

    def _log_simulated_events(self, txn_df: pd.DataFrame, meta: dict, d: date) -> None:
        label = self._label(SPECS["transactions"], d)
        ids = txn_df["txn_id"].to_numpy(dtype=object)
        for tag in ("late_arrival", "reversal", "correction"):
            sel = np.flatnonzero(meta["tag"] == tag)
            if not sel.size:
                continue
            detail = (
                [f"lag_days={int(lag)}" for lag in meta["lag"][sel]]
                if tag == "late_arrival"
                else [f"reference={r}" for r in txn_df["reference"].to_numpy(dtype=object)[sel]]
            )
            self.injector.log(
                "change_simulation", d.isoformat(), "transactions", label, ids[sel], [""] * sel.size, "*", tag, detail, [""] * sel.size, ids[sel]
            )

    def run(self) -> None:
        t0 = time.time()
        cfg = self.cfg
        (cfg.out_dir / "run_config.json").write_text(
            json.dumps(
                {
                    "seed": cfg.seed,
                    "start_date": cfg.start_date.isoformat(),
                    "days": cfg.days,
                    "scale": cfg.scale,
                    "defect_rate": cfg.defect_rate,
                    "rates": self.rates,
                    "schema_drift": cfg.schema_drift,
                    "drift_day_indexes": {
                        "new_column(accounts.account_tier)": cfg.drift_days[0],
                        "renamed_column(loans.rate->interest_rate)": cfg.drift_days[1],
                        "type_change(transactions.balance_after number->string)": cfg.drift_days[2],
                    },
                    "sim_params": SIM.__dict__,
                },
                indent=2,
            )
        )
        for i in range(cfg.days):
            self.simulate_day(i)
            if (i + 1) % 30 == 0 or i + 1 == cfg.days:
                LOG.info("day %d/%d  rows so far: %s", i + 1, cfg.days, dict(self.row_counts))
        self.injector.close()
        LOG.info("done in %.1fs | manifest rows: %d | rows written: %s", time.time() - t0, self.injector.total_logged, dict(self.row_counts))


# =============================================================================
# CLI
# =============================================================================
def parse_args(argv: Optional[Sequence[str]] = None) -> Config:
    p = argparse.ArgumentParser(description="Generate seeded synthetic Kenyan core-banking daily batch files.")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--start-date", type=date.fromisoformat, default=date(2025, 1, 1), help="first dt= partition, YYYY-MM-DD (default 2025-01-01)")
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--scale", type=float, default=1.0, help="1.0 = full run (20k customers, 30k accounts, ~3M txns); 0.05 = dev")
    p.add_argument("--defect-rate", type=float, default=0.02, help="base messiness rate; per-defect rates are multiples of it (1%%-5%% at default)")
    p.add_argument(
        "--rate",
        action="append",
        default=[],
        metavar="TYPE=VALUE",
        help=f"override one defect rate, repeatable. Types: {', '.join(DEFECT_MULTIPLIERS)}",
    )
    p.add_argument("--out-dir", type=Path, default=Path("./out"))
    p.add_argument("--no-schema-drift", action="store_true", help="disable schema drift")
    p.add_argument(
        "--drift-days",
        type=int,
        nargs=3,
        metavar=("NEWCOL", "RENAME", "TYPE"),
        help="day indexes for new column / rename / type change (default 40%%/60%%/80%% of --days)",
    )
    p.add_argument("--force", action="store_true", help="overwrite a non-empty --out-dir")
    p.add_argument("-v", "--verbose", action="store_true")
    a = p.parse_args(argv)
    if a.days < 1 or a.scale <= 0 or not 0.0 <= a.defect_rate <= 1.0:
        p.error("need --days >= 1, --scale > 0 and 0 <= --defect-rate <= 1")
    overrides: Dict[str, float] = {}
    for item in a.rate:
        if "=" not in item:
            p.error(f"--rate expects TYPE=VALUE, got '{item}'")
        k, v = item.split("=", 1)
        overrides[k.strip()] = float(v)
    try:
        resolve_rates(a.defect_rate, overrides)
    except ValueError as exc:
        p.error(str(exc))
    if (a.out_dir / "defects_manifest.csv").exists() and not a.force:
        p.error(f"{a.out_dir} already holds a run; pass --force to overwrite")
    drift = tuple(a.drift_days) if a.drift_days else (int(a.days * 0.4), int(a.days * 0.6), int(a.days * 0.8))
    logging.basicConfig(level=logging.DEBUG if a.verbose else logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    return Config(
        seed=a.seed,
        start_date=a.start_date,
        days=a.days,
        scale=a.scale,
        defect_rate=a.defect_rate,
        out_dir=a.out_dir,
        rate_overrides=overrides,
        schema_drift=not a.no_schema_drift,
        drift_days=drift,
    )


def main(argv: Optional[Sequence[str]] = None) -> int:
    cfg = parse_args(argv)
    Generator(cfg).run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
