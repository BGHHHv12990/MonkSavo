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
