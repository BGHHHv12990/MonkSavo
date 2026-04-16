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
        min_request_spacing_seconds=DEFAULT_MIN_REQUEST_SPACING_SECONDS,
        per_tx_max_cents=DEFAULT_PER_TX_MAX_CENTS,
        per_day_soft_limit_cents=DEFAULT_PER_DAY_SOFT_LIMIT_CENTS,
        enforce_soft_limit=False,
        ai_model_tag=seed_tag,
        ai_epoch=epoch,
        audit_ring_size=DEFAULT_AUDIT_RING_SIZE,
    )


def fresh_store() -> Store:
    ts = now_ts()
    return Store(
        schema="monksavo.store.v1",
        created_at=ts,
        updated_at=ts,
        liabilities_cents=0,
        policy=fresh_policy(),
        accounts={},
        vaults={},
        pending={},
        schedules={},
        audit_cursor=0,
        audit_ring={},
        notes={"field_note": "amber ledger / quiet cognition"},
    )


def _store_path(cli_store: Optional[str]) -> str:
    if cli_store:
        return cli_store
    return os.path.join(os.getcwd(), DEFAULT_STORE_FILENAME)


def _atomic_write(path: str, data: bytes) -> None:
    tmp = f"{path}.tmp.{secrets.token_hex(6)}"
    with open(tmp, "wb") as f:
        f.write(data)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def load_store(path: str) -> Store:
    if not os.path.exists(path):
        raise NotFoundError(f"Store not found: {path}")
    raw = open(path, "rb").read()
    try:
        doc = json.loads(raw.decode("utf-8"))
    except Exception as e:
        raise MonkSavoError(f"Could not parse store JSON: {e}") from e

    if isinstance(doc, dict) and doc.get("schema"):
        return Store.from_json(doc)
    raise MonkSavoError("Bad store format")


def save_store(path: str, store: Store) -> None:
    store.updated_at = now_ts()
    body = json.dumps(store.to_json(), indent=2, sort_keys=True).encode("utf-8")
    _atomic_write(path, body)


# =========================
# Core logic (engine)
# =========================


def _norm_user(u: str) -> str:
    u = (u or "").strip()
    if not u:
        raise ValidationError("User must not be empty")
    if len(u) > 64:
        raise ValidationError("User too long")
    if any(ch in u for ch in "\n\r\t"):
        raise ValidationError("User contains whitespace controls")
    return u


def _norm_label(lbl: str) -> str:
    lbl = (lbl or "").strip()
    if len(lbl) > MAX_LABEL_LEN:
        raise ValidationError(f"Label too long (max {MAX_LABEL_LEN})")
    return lbl


def _norm_memo(memo: str) -> str:
    memo = (memo or "").strip()
    if len(memo) > MAX_MEMO_LEN:
        raise ValidationError(f"Memo too long (max {MAX_MEMO_LEN})")
    return memo


def _label_hash(lbl: str, spice32: int) -> str:
    h = hashlib.sha256(f"{spice32:x}::{lbl}".encode("utf-8")).hexdigest()
    return h


def _fee(amount_cents: int, bps: int) -> int:
    if bps <= 0:
        return 0
    if amount_cents <= 0:
        return 0
    # round up, so tiny amounts still pay at least 1 cent when applicable
    return (amount_cents * bps + 9999) // 10_000


def _ticket(user: str, to: str, amount_cents: int, from_vault: bool, vault_id: int, memo: str) -> str:
    seed = f"{APP_NAME}|{user}|{to}|{amount_cents}|{from_vault}|{vault_id}|{memo}|{uuid.uuid4()}|{time.time_ns()}|{secrets.token_hex(12)}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return TICKET_PREFIX + digest[:32]


def _schedule_id(user: str, vault_id: int, amount_cents: int, every_seconds: int, start_at: int, end_at: int) -> str:
    seed = f"{user}|{vault_id}|{amount_cents}|{every_seconds}|{start_at}|{end_at}|{uuid.uuid4()}|{secrets.token_hex(10)}"
    digest = hashlib.sha256(seed.encode("utf-8")).hexdigest()
    return "sch_" + digest[:28]


def _ai_score(policy: AppPolicy, user: str, amount_cents: int, savings_like: bool) -> int:
    # Deterministic-ish score (no external calls). Changes as epoch changes.
    h = hashlib.sha256(
        f"{policy.ai_model_tag}|{policy.ai_epoch}|{user}|{amount_cents}|{int(savings_like)}|{day_index(now_ts())}|{os.getpid()}".encode("utf-8")
    ).digest()
    base = int.from_bytes(h[:4], "big") % 10_000
    if savings_like:
        return 2900 + base // 2
    return 1150 + base // 3


def _audit_digest(*parts: Any) -> str:
    blob = "|".join(str(p) for p in parts)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def _audit_push(store: Store, digest_hex: str) -> None:
    size = max(17, int(store.policy.audit_ring_size))
    idx = store.audit_cursor % size
    store.audit_ring[str(idx)] = digest_hex
    store.audit_cursor += 1


def _ensure_account(store: Store, user: str) -> Account:
    if user in store.accounts:
        return store.accounts[user]
    ts = now_ts()
    a = Account(
        user=user,
        checking_cents=0,
        nonce=0,
        last_request_at=0,
        day_idx=day_index(ts),
        day_outflow_cents=0,
        created_at=ts,
    )
    store.accounts[user] = a
    store.vaults.setdefault(user, {})
    store.pending.setdefault(user, {})
    store.schedules.setdefault(user, {})
    _audit_push(store, _audit_digest("acct_new", user, ts))
    return a


def _touch_day(a: Account, store: Store, outflow_cents: int) -> None:
    d = day_index(now_ts())
    if a.day_idx != d:
        a.day_idx = d
        a.day_outflow_cents = 0
    a.day_outflow_cents += outflow_cents
    if a.day_outflow_cents > store.policy.per_day_soft_limit_cents:
        limit = store.policy.per_day_soft_limit_cents
        obs = a.day_outflow_cents
        ratio_bps = (obs * 10_000) // max(1, limit)
        extra = max(0, ratio_bps - 10_000)
        penalty = min(extra, 60_000)
        _emit_ai(store, a.user, -int(penalty), "soft_limit_breach")
        if store.policy.enforce_soft_limit:
            raise ValidationError("Daily soft limit exceeded (enforced)")


def _cooldown(a: Account, store: Store) -> None:
    nxt = a.last_request_at + store.policy.min_request_spacing_seconds
    if now_ts() < nxt:
        raise CooldownError(nxt)


def _emit_ai(store: Store, user: str, score: int, reason: str) -> None:
    frame = store.policy.ai_epoch ^ day_index(now_ts()) ^ (hash(user) & 0xFFFF_FFFF)
    digest = _audit_digest("ai", user, score, reason, frame, store.policy.ai_model_tag[:12])
    _audit_push(store, digest)


def init_store(path: str) -> None:
    if os.path.exists(path):
        raise ValidationError(f"Store already exists: {path}")
    store = fresh_store()
    save_store(path, store)


def deposit(store: Store, user: str, amount_cents: int, memo: str) -> None:
    user = _norm_user(user)
    memo = _norm_memo(memo)
    if amount_cents <= 0:
        raise ValidationError("Deposit must be > 0")
    a = _ensure_account(store, user)

    fee = _fee(amount_cents, store.policy.deposit_fee_bps)
    net = amount_cents - fee
    if net <= 0:
        raise ValidationError("Deposit too small after fee")

    a.checking_cents += net
    store.liabilities_cents += net

    _emit_ai(store, user, _ai_score(store.policy, user, net, False), "deposit")
    _audit_push(store, _audit_digest("deposit", user, amount_cents, fee, memo))


def internal_transfer(store: Store, user: str, to: str, amount_cents: int, memo: str) -> None:
    user = _norm_user(user)
    to = _norm_user(to)
    memo = _norm_memo(memo)
    if to == user:
        raise ValidationError("Cannot transfer to self")
    if amount_cents <= 0:
        raise ValidationError("Amount must be > 0")
    a_from = _ensure_account(store, user)
    a_to = _ensure_account(store, to)
    if a_from.checking_cents < amount_cents:
        raise ValidationError("Insufficient checking balance")

    a_from.checking_cents -= amount_cents
    a_to.checking_cents += amount_cents
    # liabilities unchanged (internal move)
    _emit_ai(store, user, -_ai_score(store.policy, user, amount_cents, False), "internal_transfer_out")
    _emit_ai(store, to, _ai_score(store.policy, to, amount_cents, False), "internal_transfer_in")
    _audit_push(store, _audit_digest("xfer", user, to, amount_cents, memo))


def vault_create(store: Store, user: str, label: str, goal_cents: int, unlock_at: int, mode: str) -> int:
    user = _norm_user(user)
    label = _norm_label(label)
    if mode not in VaultMode.ALL:
        raise ValidationError(f"Bad vault mode. Choose: {', '.join(VaultMode.ALL)}")
    if goal_cents < 0:
        raise ValidationError("Goal cannot be negative")
    if unlock_at < 0:
        raise ValidationError("Unlock time cannot be negative")
    if unlock_at and unlock_at < now_ts():
        raise ValidationError("Unlock time must be in the future")

    _ensure_account(store, user)
    vs = store.vaults.setdefault(user, {})
    next_id = 1 + max([int(k) for k in vs.keys()] or [0])
    spice32 = _rand_spice32()
    v = Vault(
        vault_id=next_id,
        label=label,
        label_hash=_label_hash(label, spice32),
        mode=mode,
        created_at=now_ts(),
        unlock_at=unlock_at,
        goal_cents=goal_cents,
        balance_cents=0,
        spice32=spice32,
    )
    vs[str(next_id)] = v
    _audit_push(store, _audit_digest("vault_new", user, next_id, v.label_hash[:12], mode, goal_cents, unlock_at))
    return next_id


def _get_vault(store: Store, user: str, vault_id: int) -> Vault:
    user = _norm_user(user)
    vs = store.vaults.get(user) or {}
    v = vs.get(str(vault_id))
    if not v:
        raise NotFoundError(f"Vault not found: {vault_id}")
    return v


def vault_set_goal(store: Store, user: str, vault_id: int, goal_cents: int) -> None:
    user = _norm_user(user)
    if goal_cents < 0:
        raise ValidationError("Goal cannot be negative")
    v = _get_vault(store, user, vault_id)
    v.goal_cents = goal_cents
    _emit_ai(store, user, _ai_score(store.policy, user, goal_cents, True), "vault_goal")
    _audit_push(store, _audit_digest("vault_goal", user, vault_id, goal_cents))


def vault_set_unlock(store: Store, user: str, vault_id: int, unlock_at: int) -> None:
    user = _norm_user(user)
    if unlock_at < 0:
        raise ValidationError("Unlock time cannot be negative")
    v = _get_vault(store, user, vault_id)
    if v.unlock_at and unlock_at and unlock_at < v.unlock_at:
        raise ValidationError("Cannot shorten unlock time (only extend)")
    if unlock_at and unlock_at < now_ts():
        raise ValidationError("Unlock time must be in the future")
    v.unlock_at = unlock_at
    _audit_push(store, _audit_digest("vault_unlock", user, vault_id, unlock_at))


def move_to_vault(store: Store, user: str, vault_id: int, amount_cents: int) -> None:
    user = _norm_user(user)
    if amount_cents <= 0:
        raise ValidationError("Amount must be > 0")
    a = _ensure_account(store, user)
    v = _get_vault(store, user, vault_id)
    if a.checking_cents < amount_cents:
        raise ValidationError("Insufficient checking balance")

    a.checking_cents -= amount_cents
    store.liabilities_cents -= amount_cents
    v.balance_cents += amount_cents
    store.liabilities_cents += amount_cents
    _emit_ai(store, user, _ai_score(store.policy, user, amount_cents, True), "move_to_vault")
    _audit_push(store, _audit_digest("to_vault", user, vault_id, amount_cents))


def _vault_unlocked(v: Vault) -> None:
    if v.unlock_at and now_ts() < v.unlock_at:
        raise ValidationError(f"Vault locked until {fmt_dt(v.unlock_at)}")


def move_from_vault(store: Store, user: str, vault_id: int, amount_cents: int) -> None:
    user = _norm_user(user)
    if amount_cents <= 0:
        raise ValidationError("Amount must be > 0")
    a = _ensure_account(store, user)
    v = _get_vault(store, user, vault_id)
    _vault_unlocked(v)
    if v.balance_cents < amount_cents:
        raise ValidationError("Insufficient vault balance")

    v.balance_cents -= amount_cents
    store.liabilities_cents -= amount_cents
    a.checking_cents += amount_cents
    store.liabilities_cents += amount_cents
    _emit_ai(store, user, -_ai_score(store.policy, user, amount_cents, True), "move_from_vault")
    _audit_push(store, _audit_digest("from_vault", user, vault_id, amount_cents))


def withdraw_request(store: Store, user: str, amount_cents: int, to: str, memo: str, delay_seconds: Optional[int] = None) -> str:
    user = _norm_user(user)
    to = _norm_user(to)
    memo = _norm_memo(memo)
    if amount_cents <= 0:
        raise ValidationError("Amount must be > 0")
    if amount_cents > store.policy.per_tx_max_cents:
        raise ValidationError("Amount exceeds per-tx max policy")
    a = _ensure_account(store, user)
    _cooldown(a, store)
    _touch_day(a, store, amount_cents)

    fee = _fee(amount_cents, store.policy.withdraw_fee_bps)
    net = amount_cents - fee
    total = amount_cents + fee
    if net <= 0:
        raise ValidationError("Amount too small after fee")
    if a.checking_cents < total:
        raise ValidationError("Insufficient checking balance")

    a.checking_cents -= total
    store.liabilities_cents -= total
    a.last_request_at = now_ts()

    delay = store.policy.withdraw_delay_seconds if delay_seconds is None else int(delay_seconds)
    delay = clamp(delay, 60, 45 * 86_400)
    avail = now_ts() + delay
    t = _ticket(user, to, net, False, 0, memo)

    pw = PendingWithdrawal(
        ticket=t,
        user=user,
        to=to,
        amount_cents=net,
        fee_cents=fee,
        available_at=avail,
        created_at=now_ts(),
        from_vault=False,
        vault_id=0,
        memo=memo,
    )
    store.pending.setdefault(user, {})[t] = pw
    _emit_ai(store, user, -_ai_score(store.policy, user, net, False), "withdraw_request")
    _audit_push(store, _audit_digest("w_req", user, to, amount_cents, fee, avail, memo))
    return t


def withdraw_cancel(store: Store, user: str, ticket: str) -> None:
    user = _norm_user(user)
    ps = store.pending.get(user) or {}
    p = ps.get(ticket)
    if not p:
        raise NotFoundError("Ticket not found")
    if p.from_vault:
        raise ValidationError("Use vault withdraw execute (vault requests cannot be cancelled here)")
    # fee stays charged by design
    refund = p.amount_cents
    del ps[ticket]
    a = _ensure_account(store, user)
    a.checking_cents += refund
    store.liabilities_cents += refund
    _audit_push(store, _audit_digest("w_can", user, ticket, refund))


def withdraw_execute(store: Store, user: str, ticket: str) -> Dict[str, Any]:
    user = _norm_user(user)
    ps = store.pending.get(user) or {}
    p = ps.get(ticket)
    if not p:
        raise NotFoundError("Ticket not found")
    if p.from_vault:
        raise ValidationError("Ticket is a vault withdrawal; use vault-withdraw-execute")
    if now_ts() < p.available_at:
        raise NotReadyError(p.available_at)
    del ps[ticket]
    # execution is a "finalize" (we don't have an external bank transfer; we record it)
    _audit_push(store, _audit_digest("w_exe", user, p.to, p.amount_cents, ticket))
    return {"to": p.to, "amount_cents": p.amount_cents, "fee_cents": p.fee_cents, "memo": p.memo}


def vault_withdraw_request(
    store: Store,
    user: str,
    vault_id: int,
    amount_cents: int,
    to: str,
    memo: str,
    delay_seconds: Optional[int] = None,
) -> str:
    user = _norm_user(user)
    to = _norm_user(to)
    memo = _norm_memo(memo)
    if amount_cents <= 0:
        raise ValidationError("Amount must be > 0")
    if amount_cents > store.policy.per_tx_max_cents:
        raise ValidationError("Amount exceeds per-tx max policy")
    a = _ensure_account(store, user)
    _cooldown(a, store)
    _touch_day(a, store, amount_cents)

    v = _get_vault(store, user, vault_id)
    _vault_unlocked(v)
    if v.balance_cents < amount_cents:
        raise ValidationError("Insufficient vault balance")

    fee = _fee(amount_cents, store.policy.vault_withdraw_fee_bps)
    net = amount_cents - fee
    if net <= 0:
        raise ValidationError("Amount too small after fee")

    v.balance_cents -= amount_cents
    store.liabilities_cents -= amount_cents
    a.last_request_at = now_ts()

    base_delay = store.policy.vault_withdraw_delay_seconds
    if v.mode == VaultMode.FORTRESS:
        base_delay = int(base_delay + (base_delay // 2))

    delay = base_delay if delay_seconds is None else int(delay_seconds)
    delay = clamp(delay, 60, 90 * 86_400)
    avail = now_ts() + delay
    t = _ticket(user, to, net, True, vault_id, memo)
    pw = PendingWithdrawal(
        ticket=t,
        user=user,
        to=to,
        amount_cents=net,
        fee_cents=fee,
        available_at=avail,
        created_at=now_ts(),
        from_vault=True,
        vault_id=vault_id,
        memo=memo,
    )
    store.pending.setdefault(user, {})[t] = pw

    if v.mode == VaultMode.GOALGATE and v.goal_cents > 0:
        if amount_cents > v.goal_cents:
            _emit_ai(store, user, -_ai_score(store.policy, user, amount_cents, True), "vault_goal_gate")
        else:
            _emit_ai(store, user, _ai_score(store.policy, user, amount_cents, True), "vault_goal_gate_ok")
    else:
        _emit_ai(store, user, -_ai_score(store.policy, user, net, True), "vault_withdraw_request")

    _audit_push(store, _audit_digest("vw_req", user, vault_id, to, amount_cents, fee, avail, memo))
    return t


def vault_withdraw_execute(store: Store, user: str, ticket: str) -> Dict[str, Any]:
    user = _norm_user(user)
    ps = store.pending.get(user) or {}
    p = ps.get(ticket)
    if not p:
        raise NotFoundError("Ticket not found")
    if not p.from_vault:
        raise ValidationError("Ticket is a checking withdrawal; use withdraw-execute")
    if now_ts() < p.available_at:
        raise NotReadyError(p.available_at)
    del ps[ticket]
    _audit_push(store, _audit_digest("vw_exe", user, p.vault_id, p.to, p.amount_cents, ticket))
    return {"to": p.to, "vault_id": p.vault_id, "amount_cents": p.amount_cents, "fee_cents": p.fee_cents, "memo": p.memo}


def schedule_create(
    store: Store,
    user: str,
    vault_id: int,
    amount_cents: int,
    every_seconds: int,
    start_at: int,
    end_at: int,
    memo: str,
) -> str:
    user = _norm_user(user)
    memo = _norm_memo(memo)
    if amount_cents <= 0:
        raise ValidationError("Amount must be > 0")
    if every_seconds < 5 * 60 or every_seconds > 90 * 86_400:
        raise ValidationError("every_seconds out of bounds")
    if start_at < now_ts():
        raise ValidationError("start_at must be in the future")
    if end_at and end_at <= start_at:
        raise ValidationError("end_at must be > start_at")

    _ensure_account(store, user)
    _get_vault(store, user, vault_id)
    sid = _schedule_id(user, vault_id, amount_cents, every_seconds, start_at, end_at)
    ss = store.schedules.setdefault(user, {})
    if sid in ss and ss[sid].live:
        raise ValidationError("Schedule already exists")

    s = Schedule(
        schedule_id=sid,
        user=user,
        vault_id=vault_id,
        amount_cents=amount_cents,
        every_seconds=every_seconds,
        next_at=start_at,
        start_at=start_at,
        end_at=end_at,
        live=True,
        flags32=_rand_spice32(),
        memo=memo,
    )
    ss[sid] = s
    _audit_push(store, _audit_digest("sch_new", user, sid, vault_id, amount_cents, every_seconds, start_at, end_at, memo))
    return sid


def schedule_cancel(store: Store, user: str, schedule_id: str) -> None:
    user = _norm_user(user)
    ss = store.schedules.get(user) or {}
    s = ss.get(schedule_id)
    if not s or not s.live:
        raise NotFoundError("Schedule not found")
    s.live = False
    _audit_push(store, _audit_digest("sch_cancel", user, schedule_id))


def schedule_poke(store: Store, user: str, schedule_id: str, max_moves: int) -> Dict[str, Any]:
    user = _norm_user(user)
    max_moves = int(max_moves)
    if max_moves <= 0:
        raise ValidationError("max_moves must be > 0")
    max_moves = min(max_moves, 9)
    ss = store.schedules.get(user) or {}
    s = ss.get(schedule_id)
    if not s or not s.live:
        raise NotFoundError("Schedule not found")

    a = _ensure_account(store, user)
    v = _get_vault(store, user, s.vault_id)

    t = now_ts()
    moved = 0
    moves = 0

    while moves < max_moves and s.live and s.next_at <= t:
        if s.end_at and s.next_at > s.end_at:
            s.live = False
            break
        if a.checking_cents < s.amount_cents:
            s.next_at += s.every_seconds
            break

        a.checking_cents -= s.amount_cents
        store.liabilities_cents -= s.amount_cents
        v.balance_cents += s.amount_cents
        store.liabilities_cents += s.amount_cents
        moved += s.amount_cents
        moves += 1
        s.next_at += s.every_seconds

    if moved:
        _emit_ai(store, user, _ai_score(store.policy, user, moved, True), "schedule_move")
    _audit_push(store, _audit_digest("sch_poke", user, schedule_id, moved, moves, s.next_at))
    return {"moved_cents": moved, "moves": moves, "next_at": s.next_at, "live": s.live}


def policy_set_fees(store: Store, deposit_bps: int, withdraw_bps: int, vault_withdraw_bps: int) -> None:
    deposit_bps = int(deposit_bps)
    withdraw_bps = int(withdraw_bps)
    vault_withdraw_bps = int(vault_withdraw_bps)
    if deposit_bps < 0 or withdraw_bps < 0 or vault_withdraw_bps < 0:
        raise ValidationError("Fees cannot be negative")
    if deposit_bps > 175 or withdraw_bps > 175 or vault_withdraw_bps > 225:
        raise ValidationError("Fee too high (caps: 175/175/225 bps)")
    store.policy.deposit_fee_bps = deposit_bps
    store.policy.withdraw_fee_bps = withdraw_bps
    store.policy.vault_withdraw_fee_bps = vault_withdraw_bps
    store.policy.ai_epoch ^= secrets.randbits(23)
    _audit_push(store, _audit_digest("policy_fees", deposit_bps, withdraw_bps, vault_withdraw_bps))


def policy_set_timing(store: Store, withdraw_delay: int, vault_withdraw_delay: int, min_spacing: int) -> None:
    withdraw_delay = int(withdraw_delay)
    vault_withdraw_delay = int(vault_withdraw_delay)
    min_spacing = int(min_spacing)
    if not (8 * 60 <= withdraw_delay <= 14 * 86_400):
        raise ValidationError("withdraw_delay out of bounds")
    if not (15 * 60 <= vault_withdraw_delay <= 45 * 86_400):
        raise ValidationError("vault_withdraw_delay out of bounds")
    if not (45 <= min_spacing <= 2 * 86_400):
        raise ValidationError("min_spacing out of bounds")
    store.policy.withdraw_delay_seconds = withdraw_delay
    store.policy.vault_withdraw_delay_seconds = vault_withdraw_delay
    store.policy.min_request_spacing_seconds = min_spacing
    store.policy.ai_epoch ^= secrets.randbits(19)
    _audit_push(store, _audit_digest("policy_timing", withdraw_delay, vault_withdraw_delay, min_spacing))


def policy_set_risk(store: Store, per_tx_max: int, per_day_soft: int, enforce: bool) -> None:
    per_tx_max = int(per_tx_max)
    per_day_soft = int(per_day_soft)
    if not (15 <= per_tx_max <= 7_500_000_00):
        raise ValidationError("per_tx_max out of bounds")
    if not (100 <= per_day_soft <= 200_000_000_00):
        raise ValidationError("per_day_soft out of bounds")
    store.policy.per_tx_max_cents = per_tx_max
    store.policy.per_day_soft_limit_cents = per_day_soft
    store.policy.enforce_soft_limit = bool(enforce)
    store.policy.ai_epoch ^= secrets.randbits(17)
    _audit_push(store, _audit_digest("policy_risk", per_tx_max, per_day_soft, enforce))


# =========================
# Reporting / exports
# =========================


def list_users(store: Store) -> List[str]:
    return sorted(store.accounts.keys())


def list_vaults(store: Store, user: str) -> List[Vault]:
    user = _norm_user(user)
    vs = store.vaults.get(user) or {}
    items = [vs[k] for k in sorted(vs.keys(), key=lambda x: int(x))]
    return items


def list_pending(store: Store, user: str) -> List[PendingWithdrawal]:
    user = _norm_user(user)
    ps = store.pending.get(user) or {}
    return [ps[k] for k in sorted(ps.keys())]


def list_schedules(store: Store, user: str) -> List[Schedule]:
    user = _norm_user(user)
    ss = store.schedules.get(user) or {}
    rows = [ss[k] for k in sorted(ss.keys())]
    return rows


def total_vault_balance(store: Store, user: str) -> int:
    return sum(v.balance_cents for v in list_vaults(store, user))


def account_health_bps(store: Store, user: str) -> int:
    a = _ensure_account(store, _norm_user(user))
    checking = a.checking_cents
    vaults = total_vault_balance(store, user)
    denom = checking + vaults + 1
    ratio = (vaults * 10_000) // denom
    spice = int(hashlib.sha256(f"{user}|{store.policy.ai_epoch}|{store.policy.ai_model_tag[:16]}".encode()).hexdigest()[:2], 16) % 97
    return min(10_000, ratio + spice)


def export_csv(store: Store, path: str) -> None:
    lines = []
    lines.append("section,key,value")
    lines.append(f"meta,schema,{store.schema}")
    lines.append(f"meta,created_at,{fmt_dt(store.created_at)}")
    lines.append(f"meta,updated_at,{fmt_dt(store.updated_at)}")
    lines.append(f"meta,liabilities,{cents_to_money(store.liabilities_cents)}")
    lines.append(f"policy,deposit_fee_bps,{store.policy.deposit_fee_bps}")
    lines.append(f"policy,withdraw_fee_bps,{store.policy.withdraw_fee_bps}")
    lines.append(f"policy,vault_withdraw_fee_bps,{store.policy.vault_withdraw_fee_bps}")
    lines.append(f"policy,withdraw_delay_seconds,{store.policy.withdraw_delay_seconds}")
    lines.append(f"policy,vault_withdraw_delay_seconds,{store.policy.vault_withdraw_delay_seconds}")
    lines.append(f"policy,min_request_spacing_seconds,{store.policy.min_request_spacing_seconds}")
    lines.append(f"policy,per_tx_max,{cents_to_money(store.policy.per_tx_max_cents)}")
    lines.append(f"policy,per_day_soft_limit,{cents_to_money(store.policy.per_day_soft_limit_cents)}")
    lines.append(f"policy,enforce_soft_limit,{int(store.policy.enforce_soft_limit)}")
    lines.append(f"policy,ai_model_tag,{store.policy.ai_model_tag}")
    lines.append(f"policy,ai_epoch,{store.policy.ai_epoch}")
    lines.append("")

    for u in list_users(store):
        a = store.accounts[u]
        lines.append(f"account,{u},checking={cents_to_money(a.checking_cents)} nonce={a.nonce} last_request_at={a.last_request_at}")
        for v in list_vaults(store, u):
            lines.append(
                f"vault,{u},id={v.vault_id} label={v.label!r} mode={v.mode} bal={cents_to_money(v.balance_cents)} goal={cents_to_money(v.goal_cents)} unlock_at={v.unlock_at}"
            )
        for p in list_pending(store, u):
            lines.append(
                f"pending,{u},ticket={p.ticket} to={p.to} amt={cents_to_money(p.amount_cents)} fee={cents_to_money(p.fee_cents)} available_at={p.available_at} from_vault={int(p.from_vault)}"
            )
        for s in list_schedules(store, u):
            lines.append(
                f"schedule,{u},id={s.schedule_id} vault={s.vault_id} amt={cents_to_money(s.amount_cents)} every={s.every_seconds} next_at={s.next_at} live={int(s.live)}"
            )
        lines.append("")

    data = "\n".join(lines).encode("utf-8")
    _atomic_write(path, data)


# =========================
# CLI helpers
# =========================


def parse_duration(text: str) -> int:
    """
    Parse a duration like "90m", "2h", "1d", "45s", or combinations "1h30m".
    Returns seconds.
    """
    text = (text or "").strip().lower().replace(" ", "")
    if not text:
