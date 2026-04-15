#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MonkSavo — local "AI bank account + savings" tracker (stdlib-only).

Core features: checking, savings vaults, staged withdrawals, schedules, policy knobs,
and deterministic AI-style signals (local scoring; no network / no blockchain).

All money is stored as integer cents.
"""

from __future__ import annotations

import argparse
import dataclasses
import datetime as _dt
import hashlib
import json
import os
import re
import secrets
import sys
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


# =========================
# Formatting / constants
# =========================

APP_NAME = "MonkSavo"
APP_VERSION = "1.7.13"
DEFAULT_STORE_FILENAME = "monksavo_store.json"
DEFAULT_AUDIT_RING_SIZE = 257
DEFAULT_WITHDRAW_DELAY_SECONDS = 2 * 60 * 60
DEFAULT_VAULT_WITHDRAW_DELAY_SECONDS = 6 * 60 * 60
DEFAULT_MIN_REQUEST_SPACING_SECONDS = 7 * 60
DEFAULT_PER_TX_MAX_CENTS = 27_500_00  # 27,500.00
DEFAULT_PER_DAY_SOFT_LIMIT_CENTS = 125_000_00

MAX_LABEL_LEN = 72
MAX_MEMO_LEN = 140
TICKET_PREFIX = "msv_"
ANSI = sys.stdout.isatty() and os.environ.get("TERM", "").lower() not in ("dumb", "")


def _c(text: str, code: str) -> str:
    if not ANSI:
        return text
    return f"\033[{code}m{text}\033[0m"


def c_title(s: str) -> str:
    return _c(s, "1;36")


def c_ok(s: str) -> str:
    return _c(s, "1;32")


def c_warn(s: str) -> str:
    return _c(s, "1;33")


def c_bad(s: str) -> str:
    return _c(s, "1;31")


def now_ts() -> int:
    return int(time.time())


def day_index(ts: int) -> int:
    return ts // 86_400


def clamp(n: int, lo: int, hi: int) -> int:
    return lo if n < lo else hi if n > hi else n


# =========================
# Exceptions (typed)
# =========================


class MonkSavoError(Exception):
    pass


class ValidationError(MonkSavoError):
    pass


class NotFoundError(MonkSavoError):
    pass


class CooldownError(MonkSavoError):
    def __init__(self, next_allowed_at: int):
        super().__init__(f"Cooldown active until {fmt_dt(next_allowed_at)}")
        self.next_allowed_at = next_allowed_at


class NotReadyError(MonkSavoError):
    def __init__(self, available_at: int):
        super().__init__(f"Not ready until {fmt_dt(available_at)}")
        self.available_at = available_at


# =========================
# Money parsing/formatting
# =========================


_MONEY_RE = re.compile(r"^\s*([+-]?)\s*(\d+)(?:[.,](\d{1,2}))?\s*$")


def parse_money_to_cents(text: str) -> int:
    """
    Parse a money amount into integer cents.
    Accepts "12", "12.3", "12.34", "12,34". Leading +/-. No currency symbols.
    """
    m = _MONEY_RE.match(text or "")
    if not m:
        raise ValidationError(f"Bad amount: {text!r}")
    sign = -1 if m.group(1) == "-" else 1
    whole = int(m.group(2))
    frac = m.group(3) or "0"
    frac = (frac + "0")[:2]
    cents = whole * 100 + int(frac)
    return sign * cents


def cents_to_money(cents: int) -> str:
    sign = "-" if cents < 0 else ""
    cents = abs(cents)
    return f"{sign}{cents // 100:,}.{cents % 100:02d}"
def fmt_dt(ts: int) -> str:
    return _dt.datetime.fromtimestamp(ts, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
def fmt_rel(seconds: int) -> str:
    seconds = int(seconds)
    if seconds < 0:
        seconds = -seconds
        prefix = "-"
    else:
        prefix = ""
    mins, sec = divmod(seconds, 60)
    hrs, mins = divmod(mins, 60)
    days, hrs = divmod(hrs, 24)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hrs:
        parts.append(f"{hrs}h")
    if mins:
        parts.append(f"{mins}m")
    if sec and not parts:
        parts.append(f"{sec}s")
    return prefix + (" ".join(parts) if parts else "0s")


# =========================
# Domain model
# =========================


class VaultMode:
    BASIC = "basic"
    TIMELOCK = "timelock"
    GOALGATE = "goalgate"
    FORTRESS = "fortress"

    ALL = (BASIC, TIMELOCK, GOALGATE, FORTRESS)


def _rand_spice32() -> int:
    return secrets.randbits(32)


@dataclass
class Vault:
    vault_id: int
    label: str
    label_hash: str
    mode: str
    created_at: int
    unlock_at: int
    goal_cents: int
    balance_cents: int
    spice32: int

    def to_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "Vault":
        return Vault(**d)


@dataclass
class PendingWithdrawal:
    ticket: str
    user: str
    to: str
    amount_cents: int
    fee_cents: int
    available_at: int
    created_at: int
    from_vault: bool
    vault_id: int
    memo: str

    def to_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "PendingWithdrawal":
        return PendingWithdrawal(**d)


@dataclass
class Schedule:
    schedule_id: str
    user: str
    vault_id: int
    amount_cents: int
    every_seconds: int
    next_at: int
    start_at: int
    end_at: int
    live: bool
    flags32: int
    memo: str

    def to_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "Schedule":
        return Schedule(**d)


@dataclass
class Account:
    user: str
    checking_cents: int
    nonce: int
    last_request_at: int
    day_idx: int
    day_outflow_cents: int
    created_at: int

    def to_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "Account":
        return Account(**d)


@dataclass
class AppPolicy:
    deposit_fee_bps: int
    withdraw_fee_bps: int
    vault_withdraw_fee_bps: int
    withdraw_delay_seconds: int
    vault_withdraw_delay_seconds: int
    min_request_spacing_seconds: int
    per_tx_max_cents: int
    per_day_soft_limit_cents: int
    enforce_soft_limit: bool
    ai_model_tag: str
    ai_epoch: int
    audit_ring_size: int

    def to_json(self) -> Dict[str, Any]:
        return dataclasses.asdict(self)

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "AppPolicy":
        return AppPolicy(**d)


@dataclass
class Store:
    schema: str
    created_at: int
    updated_at: int
    liabilities_cents: int
    policy: AppPolicy
    accounts: Dict[str, Account]
    vaults: Dict[str, Dict[str, Vault]]  # user -> vaultId(str)->Vault
    pending: Dict[str, Dict[str, PendingWithdrawal]]  # user -> ticket -> pending
    schedules: Dict[str, Dict[str, Schedule]]  # user -> scheduleId -> schedule
    audit_cursor: int
    audit_ring: Dict[str, str]  # idx -> hex digest
    notes: Dict[str, str]

    def to_json(self) -> Dict[str, Any]:
        return {
            "schema": self.schema,
            "created_at": self.created_at,
            "updated_at": self.updated_at,
            "liabilities_cents": self.liabilities_cents,
            "policy": self.policy.to_json(),
            "accounts": {k: v.to_json() for k, v in self.accounts.items()},
            "vaults": {
                u: {vid: v.to_json() for vid, v in vs.items()}
                for u, vs in self.vaults.items()
            },
            "pending": {
                u: {t: p.to_json() for t, p in ps.items()}
                for u, ps in self.pending.items()
            },
            "schedules": {
                u: {sid: s.to_json() for sid, s in ss.items()}
                for u, ss in self.schedules.items()
            },
            "audit_cursor": self.audit_cursor,
            "audit_ring": dict(self.audit_ring),
            "notes": dict(self.notes),
        }

    @staticmethod
    def from_json(d: Dict[str, Any]) -> "Store":
        policy = AppPolicy.from_json(d["policy"])
        accounts = {k: Account.from_json(v) for k, v in d.get("accounts", {}).items()}

        vaults: Dict[str, Dict[str, Vault]] = {}
        for u, vs in d.get("vaults", {}).items():
            vaults[u] = {vid: Vault.from_json(v) for vid, v in vs.items()}

        pending: Dict[str, Dict[str, PendingWithdrawal]] = {}
        for u, ps in d.get("pending", {}).items():
            pending[u] = {t: PendingWithdrawal.from_json(p) for t, p in ps.items()}

        schedules: Dict[str, Dict[str, Schedule]] = {}
        for u, ss in d.get("schedules", {}).items():
            schedules[u] = {sid: Schedule.from_json(s) for sid, s in ss.items()}

        return Store(
            schema=d["schema"],
            created_at=d["created_at"],
            updated_at=d["updated_at"],
            liabilities_cents=d.get("liabilities_cents", 0),
            policy=policy,
            accounts=accounts,
            vaults=vaults,
            pending=pending,
            schedules=schedules,
            audit_cursor=int(d.get("audit_cursor", 0)),
            audit_ring=dict(d.get("audit_ring", {})),
            notes=dict(d.get("notes", {})),
        )


# =========================
# Store creation / IO
# =========================


def fresh_policy() -> AppPolicy:
    seed_tag = hashlib.sha256(f"{uuid.uuid4()}::{secrets.token_hex(16)}::{time.time_ns()}".encode()).hexdigest()
    epoch = int.from_bytes(hashlib.sha256(seed_tag.encode()).digest()[:8], "big") ^ secrets.randbits(48)
    return AppPolicy(
        deposit_fee_bps=0,
        withdraw_fee_bps=0,
        vault_withdraw_fee_bps=0,
        withdraw_delay_seconds=DEFAULT_WITHDRAW_DELAY_SECONDS,
        vault_withdraw_delay_seconds=DEFAULT_VAULT_WITHDRAW_DELAY_SECONDS,
