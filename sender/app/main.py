"""发布值班发送端。

职责：登记“部署完成通知”，调用审计接收端接口，并仅凭接收端回执确认送达。
原则：
- 每次发送尝试先落库（含唯一 attempt_id），落库失败则绝不发出请求；
- 发送前先把“已发出、结果未知（待核）”落库，进程崩溃也不会丢失该事实；
- 只有拿到接收端内容一致的回执才置为“已确认”，绝不虚报成功；
- 接收端在首次入账时保存 attempt_id；重试返回原标识，据此可归因“哪次尝试首次入账”；
- 丢回执 / 接收端不可达 / 发送端重启后，均可凭接收端回执核对补账、补全归因。
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import sqlite3
import threading
import time
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import httpx
from fastapi import FastAPI, Query
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel

DB_PATH = os.environ.get("DB_PATH", "/data/sender.db")
RECEIVER_URL = os.environ.get("RECEIVER_URL", "http://localhost:8001").rstrip("/")
HTTP_TIMEOUT = float(os.environ.get("HTTP_TIMEOUT", "3"))
STARTUP_RECONCILE = os.environ.get("STARTUP_RECONCILE", "1") == "1"
STATIC_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static")

STATUS_PENDING = "PENDING"        # 待发送：已登记，尚未发送或已确认对方未记账
STATUS_UNCERTAIN = "UNCERTAIN"    # 待核：已发出但回执缺失/对方不可达，是否送达未知
STATUS_ACKED = "ACKED"            # 已确认：收到接收端内容一致的回执
STATUS_CONFLICT = "CONFLICT"      # 冲突：同环境同发布号在接收端已存在不同内容

# 尝试结果
OUTCOME_IN_FLIGHT = "IN_FLIGHT"            # 已落库，请求进行中（进程崩溃则由重启标记为 INTERRUPTED）
OUTCOME_RECORDED = "RECORDED"              # 接收端本次首次入账
OUTCOME_DEDUP = "DEDUP"                    # 接收端已有相同内容记录（幂等返回）
OUTCOME_CONFLICT = "CONFLICT"              # 接收端返回 409
OUTCOME_CONTENT_MISMATCH = "CONTENT_MISMATCH"
OUTCOME_HTTP_ERROR = "HTTP_ERROR"          # 非 2xx/409
OUTCOME_UNREACHABLE = "UNREACHABLE"        # 连接失败 / 超时，回执未知
OUTCOME_INTERRUPTED = "INTERRUPTED"        # 发送端重启时仍在进行，结果未知

# 归因状态（哪次尝试让接收端首次入账）
ATTR_NONE = "NONE"                  # 尚无回执（待发送）
ATTR_UNKNOWN = "UNKNOWN"            # 回执/归因未知（待核或接收端失联）
ATTR_TRACED = "TRACED"              # 已归因到本端某次尝试
ATTR_UNTRACEABLE = "UNTRACEABLE"    # 回执没有尝试标识（旧回执/旧客户端）
ATTR_FOREIGN = "FOREIGN"            # 回执标识不属于本端尝试记录

VALID_SIMULATE = {"lose_response", "slow"}
# 发送端专用故障注入（用于验证“尝试记录落库失败则绝不发送”）
SIMULATE_FAIL_ATTEMPT_DB = "fail_attempt_db"
VALID_SENDER_SIMULATE = VALID_SIMULATE | {SIMULATE_FAIL_ATTEMPT_DB}
ATTEMPT_ID_RE = re.compile(r"^[A-Za-z0-9_-]{1,64}$")


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def content_hash(env: str, release_no: str, result: str) -> str:
    canonical = json.dumps(
        {"env": env, "release_no": release_no, "result": result},
        ensure_ascii=False,
        sort_keys=True,
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, isolation_level=None, timeout=10)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=5000")
    return conn


def _ensure_column(conn: sqlite3.Connection, table: str, column: str, decl: str) -> None:
    cols = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db() -> None:
    os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
    conn = connect()
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                env TEXT NOT NULL,
                release_no TEXT NOT NULL,
                result TEXT NOT NULL,
                content_hash TEXT NOT NULL,
                status TEXT NOT NULL,
                attempt_count INTEGER NOT NULL DEFAULT 0,
                receipt_id INTEGER,
                receipt_at TEXT,
                receipt_seen INTEGER NOT NULL DEFAULT 0,
                receipt_first_attempt_id TEXT,
                last_error TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(env, release_no)
            )
            """
        )
        # 旧库迁移：补列，并把曾收到过回执的记录标为“已见回执”（无标识 → 无法追溯）
        _ensure_column(conn, "notifications", "receipt_seen", "INTEGER NOT NULL DEFAULT 0")
        _ensure_column(conn, "notifications", "receipt_first_attempt_id", "TEXT")
        conn.execute(
            "UPDATE notifications SET receipt_seen=1 WHERE receipt_id IS NOT NULL AND receipt_seen=0"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS attempts (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                notification_id INTEGER NOT NULL,
                attempt_no INTEGER NOT NULL,
                attempt_id TEXT NOT NULL UNIQUE,
                outcome TEXT NOT NULL,
                http_status INTEGER,
                detail TEXT,
                started_at TEXT NOT NULL,
                finished_at TEXT,
                UNIQUE(notification_id, attempt_no)
            )
            """
        )
        # 进程重启：上次停在“进行中”的尝试已不可能拿到回执结论，如实标记为结果未知
        conn.execute(
            """UPDATE attempts SET outcome=?, finished_at=COALESCE(finished_at, ?),
                      detail=COALESCE(detail, '发送端重启时该尝试仍在进行，结果未知')
               WHERE outcome=?""",
            (OUTCOME_INTERRUPTED, now_iso(), OUTCOME_IN_FLIGHT),
        )
    finally:
        conn.close()


def row_to_dict(row: sqlite3.Row) -> dict:
    return {k: row[k] for k in row.keys()}


def set_fields(conn: sqlite3.Connection, nid: int, **fields) -> None:
    cols = ", ".join(f"{k}=?" for k in fields)
    conn.execute(
        f"UPDATE notifications SET {cols}, updated_at=? WHERE id=?",
        (*fields.values(), now_iso(), nid),
    )


def set_fields_unless_acked(conn: sqlite3.Connection, nid: int, **fields) -> bool:
    """更新字段，但不把“已确认”记录改回未结案状态（并发交错时避免误降级）。"""
    cols = ", ".join(f"{k}=?" for k in fields)
    cur = conn.execute(
        f"UPDATE notifications SET {cols}, updated_at=? WHERE id=? AND status<>?",
        (*fields.values(), now_iso(), nid, STATUS_ACKED),
    )
    return cur.rowcount > 0


def set_fields_if_open(conn: sqlite3.Connection, nid: int, **fields) -> bool:
    """仅当记录仍未结案（待发送/待核）时更新；已确认/冲突不被降级或改写。"""
    cols = ", ".join(f"{k}=?" for k in fields)
    cur = conn.execute(
        f"UPDATE notifications SET {cols}, updated_at=? WHERE id=? AND status IN (?, ?)",
        (*fields.values(), now_iso(), nid, STATUS_UNCERTAIN, STATUS_PENDING),
    )
    return cur.rowcount > 0


def _attribution(row: dict, attempts: list[dict]) -> dict:
    """计算“哪次尝试让接收端首次入账”。只认接收端回执里的 first_attempt_id。"""
    if not row.get("receipt_seen"):
        if row["status"] in (STATUS_UNCERTAIN, STATUS_ACKED, STATUS_CONFLICT):
            state = ATTR_UNKNOWN
        else:
            state = ATTR_NONE
        return {"attribution": state, "first_attempt_id": None, "first_attempt_no": None}

    faid = row.get("receipt_first_attempt_id")
    if not faid:
        # 旧回执 / 旧客户端没有尝试标识，无法追溯
        return {"attribution": ATTR_UNTRACEABLE, "first_attempt_id": None, "first_attempt_no": None}
    for a in attempts:
        if a["attempt_id"] == faid:
            return {"attribution": ATTR_TRACED, "first_attempt_id": faid,
                    "first_attempt_no": a["attempt_no"]}
    return {"attribution": ATTR_FOREIGN, "first_attempt_id": faid, "first_attempt_no": None}


def notification_view(conn: sqlite3.Connection, nid: int) -> dict | None:
    row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
    if row is None:
        return None
    out = row_to_dict(row)
    attempts = [
        row_to_dict(r)
        for r in conn.execute(
            "SELECT * FROM attempts WHERE notification_id=? ORDER BY attempt_no", (nid,)
        ).fetchall()
    ]
    out["attempts"] = attempts
    out.update(_attribution(out, attempts))
    return out


# ---------------- 核心动作 ----------------

def _finalize_attempt(
    conn: sqlite3.Connection,
    nid: int,
    token: str,
    outcome: str,
    http_status: int | None,
    detail: str | None,
    notify_fields: dict | None = None,
) -> tuple[int, dict]:
    """落定一次尝试的结果。绝不把已有“已确认”降级为待核/待发送。"""
    notify_fields = notify_fields or {}
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute(
            "UPDATE attempts SET outcome=?, http_status=?, detail=?, finished_at=? WHERE attempt_id=?",
            (outcome, http_status, (detail or "")[:300] or None, now_iso(), token),
        )
        current = conn.execute("SELECT status FROM notifications WHERE id=?", (nid,)).fetchone()
        if current is not None:
            target = notify_fields.get("status")
            if target is None or target == STATUS_ACKED:
                set_fields(conn, nid, **notify_fields)
            elif target == STATUS_CONFLICT:
                # 冲突只可由未结案记录进入，已确认不被降级
                set_fields_unless_acked(conn, nid, **notify_fields)
            else:
                # 待核/待发送等“未知”结论：绝不覆盖已确认/已冲突的既有定案
                set_fields_if_open(conn, nid, **notify_fields)
        conn.execute("COMMIT")
    except sqlite3.Error as exc:
        try:
            conn.execute("ROLLBACK")
        except sqlite3.Error:
            pass
        out = notification_view(conn, nid) or {}
        out["note"] = f"尝试结果未能写回本地（可稍后核对）：{exc}"
        return 500, out
    return 200, notification_view(conn, nid) or {}


def do_send(nid: int, simulate: str | None = None) -> tuple[int, dict]:
    """发送（或重发）一条通知。只有拿到接收端内容一致的回执才置为已确认。"""
    conn = connect()
    try:
        started = now_iso()
        # 1) 尝试记录与“待核”状态同一事务落库；落库失败则绝不发出请求，
        #    否则接收端可能已入账而发送端没有任何尝试可用于归因。
        try:
            conn.execute("BEGIN IMMEDIATE")
            row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
            if row is None:
                conn.execute("ROLLBACK")
                return 404, {"detail": "通知不存在"}
            if row["status"] == STATUS_ACKED:
                conn.execute("COMMIT")
                out = notification_view(conn, nid) or {}
                out["note"] = "已确认，无需重复发送"
                return 200, out
            attempt_no = conn.execute(
                "SELECT COALESCE(MAX(attempt_no), 0) + 1 AS n FROM attempts WHERE notification_id=?",
                (nid,),
            ).fetchone()["n"]
            token = uuid.uuid4().hex
            if simulate == SIMULATE_FAIL_ATTEMPT_DB:
                # 故障注入（仅演示）：模拟尝试记录落库失败。
                raise sqlite3.OperationalError("模拟故障：尝试记录落库失败")
            conn.execute(
                """INSERT INTO attempts
                   (notification_id, attempt_no, attempt_id, outcome, started_at)
                   VALUES (?,?,?,?,?)""",
                (nid, attempt_no, token, OUTCOME_IN_FLIGHT, started),
            )
            # 即使此刻进程崩溃，重启后也知道必须核对，而不是误以为未发或误报成功。
            conn.execute(
                "UPDATE notifications SET status=?, attempt_count=attempt_count+1, updated_at=? WHERE id=?",
                (STATUS_UNCERTAIN, now_iso(), nid),
            )
            conn.execute("COMMIT")
        except sqlite3.Error as exc:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            return 500, {"detail": f"尝试记录落库失败，未向接收端发送：{exc}"}

        # 2) 调用接收端（带本次尝试标识；接收端首次入账时保存它）
        payload = {
            "env": row["env"],
            "release_no": row["release_no"],
            "result": row["result"],
            "attempt_id": token,
        }
        if simulate:
            payload["simulate"] = simulate
        try:
            resp = httpx.post(f"{RECEIVER_URL}/api/receipts", json=payload, timeout=HTTP_TIMEOUT)
        except httpx.HTTPError as exc:
            return _finalize_attempt(
                conn, nid, token, OUTCOME_UNREACHABLE, None, exc.__class__.__name__,
                {"status": STATUS_UNCERTAIN,
                 "last_error": f"接收端不可达或回执丢失（{exc.__class__.__name__}）"},
            )

        # 3) 只有接收端 2xx 回执且内容一致才算送达；回执里的 first_attempt_id 即归因依据
        if resp.status_code in (200, 201):
            receipt = resp.json()
            if receipt.get("content_hash") == row["content_hash"]:
                recorded = resp.status_code == 201 and not receipt.get("deduplicated")
                outcome = OUTCOME_RECORDED if recorded else OUTCOME_DEDUP
                return _finalize_attempt(
                    conn, nid, token, outcome, resp.status_code,
                    "接收端本次首次入账" if recorded else "接收端已有相同内容记录（幂等返回原回执）",
                    {"status": STATUS_ACKED, "last_error": None,
                     "receipt_id": receipt.get("id"), "receipt_at": receipt.get("received_at"),
                     "receipt_seen": 1,
                     "receipt_first_attempt_id": receipt.get("first_attempt_id")},
                )
            return _finalize_attempt(
                conn, nid, token, OUTCOME_CONTENT_MISMATCH, resp.status_code,
                "回执内容与本地登记不一致",
                {"status": STATUS_CONFLICT, "last_error": "回执内容与本地登记不一致",
                 "receipt_id": receipt.get("id"), "receipt_at": receipt.get("received_at"),
                 "receipt_seen": 1,
                 "receipt_first_attempt_id": receipt.get("first_attempt_id")},
            )
        if resp.status_code == 409:
            body = resp.json()
            existing = body.get("existing") or {}
            return _finalize_attempt(
                conn, nid, token, OUTCOME_CONFLICT, 409,
                body.get("detail", "接收端已存在不同内容"),
                {"status": STATUS_CONFLICT,
                 "last_error": body.get("detail", "接收端已存在不同内容"),
                 "receipt_id": existing.get("id"), "receipt_at": existing.get("received_at"),
                 "receipt_seen": 1,
                 "receipt_first_attempt_id": existing.get("first_attempt_id")},
            )
        return _finalize_attempt(
            conn, nid, token, OUTCOME_HTTP_ERROR, resp.status_code,
            f"接收端返回 {resp.status_code}",
            {"status": STATUS_UNCERTAIN, "last_error": f"接收端返回 {resp.status_code}，回执缺失"},
        )
    finally:
        conn.close()


def do_reconcile(nid: int) -> tuple[int, dict]:
    """凭接收端回执核对真实状态并补全归因。"""
    conn = connect()
    try:
        row = conn.execute("SELECT * FROM notifications WHERE id=?", (nid,)).fetchone()
        if row is None:
            return 404, {"detail": "通知不存在"}
        try:
            resp = httpx.get(
                f"{RECEIVER_URL}/api/receipts",
                params={"env": row["env"], "release_no": row["release_no"]},
                timeout=HTTP_TIMEOUT,
            )
        except httpx.HTTPError:
            # 接收端暂不可查：保持现状（待核仍为待核），归因显示未知，绝不虚报成功
            set_fields_unless_acked(conn, nid, last_error="核对待定：接收端暂不可达")
            out = notification_view(conn, nid) or {}
            out["note"] = "接收端暂不可达，状态保持不变，首次入账尝试未知"
            return 200, out

        if resp.status_code == 404:
            # 接收端明确无此记录：未送达，可安全重发。
            # 用条件更新防止把并发中刚被另一次核对确认的记录降级。
            reset = set_fields_unless_acked(
                conn, nid, status=STATUS_PENDING, last_error=None,
                receipt_id=None, receipt_at=None, receipt_seen=0,
                receipt_first_attempt_id=None,
            )
            if not reset:
                set_fields(conn, nid, last_error="接收端当前无此记录（此前已收到回执）")
            out = notification_view(conn, nid) or {}
            out["note"] = "接收端无此记录，可安全重发"
            return 200, out
        if resp.status_code == 200:
            receipt = resp.json()
            first_attempt_id = receipt.get("first_attempt_id")
            if receipt.get("content_hash") == row["content_hash"]:
                set_fields(conn, nid, status=STATUS_ACKED, last_error=None,
                           receipt_id=receipt.get("id"), receipt_at=receipt.get("received_at"),
                           receipt_seen=1, receipt_first_attempt_id=first_attempt_id)
            else:
                set_fields(conn, nid, status=STATUS_CONFLICT,
                           last_error="接收端已存在同环境同发布号的不同内容",
                           receipt_id=receipt.get("id"), receipt_at=receipt.get("received_at"),
                           receipt_seen=1, receipt_first_attempt_id=first_attempt_id)
            return 200, notification_view(conn, nid) or {}

        set_fields(conn, nid, last_error=f"核对失败：接收端返回 {resp.status_code}")
        return 200, notification_view(conn, nid) or {}
    finally:
        conn.close()


def reconcile_all_open() -> int:
    """核对所有未结案（待核/待发送）记录，返回处理条数。

    跳过仍有请求在途的记录：启动核对线程可能在发送请求进行中就运行，
    此时抢答会把在途尝试误判为“接收端无记录”。
    """
    conn = connect()
    try:
        rows = conn.execute(
            """SELECT n.id FROM notifications n
               WHERE n.status IN (?, ?)
                 AND NOT EXISTS (
                     SELECT 1 FROM attempts a
                     WHERE a.notification_id = n.id AND a.outcome = ?
                 )""",
            (STATUS_UNCERTAIN, STATUS_PENDING, OUTCOME_IN_FLIGHT),
        ).fetchall()
    finally:
        conn.close()
    for r in rows:
        do_reconcile(r["id"])
    return len(rows)


def _startup_reconcile() -> None:
    """发送端重启后主动核对所有待核记录：只凭接收端回执定案并补全归因。"""
    time.sleep(2)  # 等待接收端就绪
    try:
        n = reconcile_all_open()
        print(f"[startup] 重启核对完成，共处理 {n} 条待核/待发送记录", flush=True)
    except Exception as exc:  # noqa: BLE001 - 启动核对失败不致命，可稍后手动核对
        print(f"[startup] 重启核对失败（可稍后手动核对）: {exc}", flush=True)


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    if STARTUP_RECONCILE:
        threading.Thread(target=_startup_reconcile, daemon=True).start()
    yield


app = FastAPI(title="发布值班发送端", lifespan=lifespan)


class NotificationIn(BaseModel):
    env: str
    release_no: str
    result: str


@app.get("/api/health")
def health():
    return {"ok": True}


@app.post("/api/notifications")
def create_notification(body: NotificationIn):
    """登记通知。同 (env, release_no) 同内容幂等去重，内容不同报 409 冲突。"""
    env, release_no, result = body.env.strip(), body.release_no.strip(), body.result.strip()
    if not env or not release_no or not result:
        return JSONResponse(status_code=400, content={"detail": "env / release_no / result 均不能为空"})
    if len(env) > 64 or len(release_no) > 64 or len(result) > 64:
        return JSONResponse(status_code=400, content={"detail": "字段长度不能超过 64 字符"})

    ch = content_hash(env, release_no, result)
    conn = connect()
    try:
        conn.execute("BEGIN IMMEDIATE")
        try:
            row = conn.execute(
                "SELECT * FROM notifications WHERE env=? AND release_no=?", (env, release_no)
            ).fetchone()
            if row is not None:
                conn.execute("COMMIT")
                if row["content_hash"] == ch:
                    out = notification_view(conn, row["id"]) or {}
                    out["deduplicated"] = True
                    return JSONResponse(status_code=200, content=out)
                return JSONResponse(status_code=409, content={
                    "detail": "相同环境与发布号已存在不同内容",
                    "existing": notification_view(conn, row["id"]) or {},
                })
            now = now_iso()
            cur = conn.execute(
                """INSERT INTO notifications
                   (env, release_no, result, content_hash, status, attempt_count, created_at, updated_at)
                   VALUES (?,?,?,?,?,0,?,?)""",
                (env, release_no, result, ch, STATUS_PENDING, now, now),
            )
            nid = cur.lastrowid
            conn.execute("COMMIT")
        except sqlite3.Error:
            try:
                conn.execute("ROLLBACK")
            except sqlite3.Error:
                pass
            raise
        return JSONResponse(status_code=201, content=notification_view(conn, nid) or {})
    except sqlite3.Error as exc:
        return JSONResponse(status_code=500, content={"detail": f"本地写入失败：{exc}"})
    finally:
        conn.close()


@app.get("/api/notifications")
def list_notifications():
    conn = connect()
    try:
        rows = conn.execute("SELECT id FROM notifications ORDER BY id DESC").fetchall()
        return {"items": [notification_view(conn, r["id"]) for r in rows]}
    finally:
        conn.close()


@app.get("/api/notifications/{nid}")
def get_notification(nid: int):
    conn = connect()
    try:
        out = notification_view(conn, nid)
        if out is None:
            return JSONResponse(status_code=404, content={"detail": "通知不存在"})
        return out
    finally:
        conn.close()


@app.post("/api/notifications/{nid}/send")
def send_notification(nid: int, simulate: str | None = Query(default=None)):
    if simulate is not None and simulate not in VALID_SENDER_SIMULATE:
        return JSONResponse(status_code=400, content={"detail": f"simulate 仅支持：{sorted(VALID_SENDER_SIMULATE)}"})
    code, payload = do_send(nid, simulate)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/notifications/{nid}/reconcile")
def reconcile_notification(nid: int):
    code, payload = do_reconcile(nid)
    return JSONResponse(status_code=code, content=payload)


@app.post("/api/reconcile-all")
def reconcile_all_endpoint():
    n = reconcile_all_open()
    return {"reconciled": n}


@app.get("/")
def index():
    return FileResponse(os.path.join(STATIC_DIR, "index.html"))
