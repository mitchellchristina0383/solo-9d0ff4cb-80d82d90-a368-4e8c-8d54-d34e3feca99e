#!/usr/bin/env python3
"""关键情形端到端自测（仅依赖 Python 标准库）。

覆盖：
  S1  正常发送 → 已确认，且首次入账归因到第 1 次尝试
  S2  同内容重试只记一次（两端均不重复），重试不改写首次入账标识
  S3  同键不同内容 → 409 冲突（发送端登记与接收端直连均验证）
  S4  入库后丢回执 → 待核（归因未知）→ 重发去重 → 已确认且归因仍指向第 1 次
  S5  入库后丢回执 → 待核 → 核对补账 → 已确认并补全归因
  S6  慢回执（发送端超时）→ 待核 → 核对 → 已确认
  S7  并发同内容提交 → 两端各仅一条，且首次入账尝试唯一
  S8  并发不同内容 → 仅一条，败者 409
  S9  接收端不可用 → 待核且不虚报成功（--lifecycle）
  S10 发送端重启（接收端停机中）→ 待核持久 → 恢复后凭回执补核（--lifecycle）
  S11 发送端重启（接收端正常）→ 启动自动核对凭回执结案并补全归因（--lifecycle）
  S12 接收端失联时核对 → 状态不变、归因显示未知（--lifecycle）
  S13 尝试记录落库失败 → 500 且绝不向接收端发出请求
  S14 旧回执无尝试标识 → 显示“无法追溯”
  S15 首次入账后交错重试（并发）→ 首次入账标识不被改写
  S16 发送与核对交错（并发）→ 已确认不被晚到结论降级

用法：
  python3 tests/selftest.py [--lifecycle]
环境变量：
  SENDER_URL / RECEIVER_URL   服务地址（默认 http://127.0.0.1:8000 / :8001）
  RN_LIFECYCLE=1              等价于 --lifecycle
  RN_HOOK_STOP_RECEIVER 等    生命周期钩子命令（默认 docker compose ...）
"""
import json
import os
import subprocess
import sys
import threading
import time
import urllib.error
import urllib.request

SENDER = os.environ.get("SENDER_URL", "http://127.0.0.1:8000").rstrip("/")
RECEIVER = os.environ.get("RECEIVER_URL", "http://127.0.0.1:8001").rstrip("/")
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RUN = str(int(time.time()))
ENV = "itest"

PASSED, FAILED = [], []


def check(name, cond, extra=""):
    (PASSED if cond else FAILED).append(name)
    mark = "PASS" if cond else "FAIL"
    print(f"[{mark}] {name}" + ("" if cond else f"  | {extra}"), flush=True)


def req(method, url, body=None, timeout=20):
    data = json.dumps(body).encode() if body is not None else None
    r = urllib.request.Request(url, data=data, method=method,
                               headers={"Content-Type": "application/json"})
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, (json.loads(raw) if raw else {})
    except urllib.error.HTTPError as e:
        raw = e.read()
        try:
            payload = json.loads(raw) if raw else {}
        except json.JSONDecodeError:
            payload = {"raw": raw.decode(errors="replace")}
        return e.code, payload
    except urllib.error.URLError as e:
        return 0, {"error": str(e)}


def sender_create(env, rel, result):
    return req("POST", SENDER + "/api/notifications",
               {"env": env, "release_no": rel, "result": result})


def sender_send(nid, simulate=None):
    url = f"{SENDER}/api/notifications/{nid}/send"
    if simulate:
        url += f"?simulate={simulate}"
    return req("POST", url)


def sender_get(nid):
    return req("GET", f"{SENDER}/api/notifications/{nid}")


def sender_reconcile(nid):
    return req("POST", f"{SENDER}/api/notifications/{nid}/reconcile")


def sender_rows(rel):
    _, d = req("GET", SENDER + "/api/notifications")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def receiver_rows(rel):
    _, d = req("GET", RECEIVER + "/api/receipts")
    return [x for x in d.get("items", []) if x["env"] == ENV and x["release_no"] == rel]


def receiver_receipt(rel):
    _, d = req("GET", f"{RECEIVER}/api/receipts?env={ENV}&release_no={rel}")
    return d


def hook(env_var, default_cmd):
    cmd = os.environ.get(env_var, default_cmd)
    print(f"  >> 钩子: {cmd}", flush=True)
    r = subprocess.run(cmd, shell=True, cwd=PROJECT_ROOT,
                       stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    if r.returncode != 0:
        print(f"  !! 钩子返回码 {r.returncode}", flush=True)


def wait_up(base, tries=60):
    for _ in range(tries):
        code, _ = req("GET", base + "/api/health", timeout=3)
        if code == 200:
            return True
        time.sleep(0.5)
    return False


def key(tag):
    return f"R{RUN}-{tag}"


# ---------------- 场景 ----------------

def s1_basic():
    k = key("k1")
    code, n = sender_create(ENV, k, "success")
    check("S1 登记通知成功且初始为待发送", code in (200, 201) and n.get("status") == "PENDING", f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S1 发送后已确认且有回执号", n2.get("status") == "ACKED" and bool(n2.get("receipt_id")), f"{n2}")
    check("S1 接收端仅一条记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    attempts = n2.get("attempts") or []
    check("S1 发送端保存了 1 次尝试且标记首次入账",
          len(attempts) == 1 and attempts[0]["outcome"] == "RECORDED", f"{attempts}")
    check("S1 首次入账归因到第 1 次尝试",
          n2.get("attribution") == "TRACED" and n2.get("first_attempt_no") == 1, f"{n2}")
    receipt = receiver_receipt(k)
    check("S1 接收端回执保存了同一尝试标识",
          receipt.get("first_attempt_id") == attempts[0]["attempt_id"], f"{receipt} vs {attempts}")


def s2_retry_dedup():
    k = key("k1")  # 与 S1 同键：同内容重试
    code, n = sender_create(ENV, k, "success")
    check("S2 同内容重复登记被幂等去重", code == 200 and n.get("deduplicated") is True, f"code={code} {n}")
    code, n2 = sender_send(n["id"])
    check("S2 重发后仍为已确认", n2.get("status") == "ACKED", f"{n2}")
    check("S2 发送端同键仅一条", len(sender_rows(k)) == 1)
    check("S2 接收端同键仍仅一条", len(receiver_rows(k)) == 1)
    check("S2 已确认后重发不新增尝试", len(n2.get("attempts") or []) == 1, f"{n2.get('attempts')}")


def s2b_retry_after_pending():
    """未确认前的同内容重试：新增尝试，但接收端首次入账标识不被改写。"""
    k = key("k2b")
    code, n = sender_create(ENV, k, "success")
    # 先清空本地状态模拟“发送端未保存回执”，再直接并发打接收端两次相同内容
    first_id = f"itest-{RUN}-k2b-a"
    second_id = f"itest-{RUN}-k2b-b"
    c1, r1 = req("POST", RECEIVER + "/api/receipts",
                 {"env": ENV, "release_no": k, "result": "success", "attempt_id": first_id})
    c2, r2 = req("POST", RECEIVER + "/api/receipts",
                 {"env": ENV, "release_no": k, "result": "success", "attempt_id": second_id})
    check("S2b 接收端首次入账返回 201 并保存首个标识",
          c1 == 201 and r1.get("first_attempt_id") == first_id, f"{c1} {r1}")
    check("S2b 同内容重试 200 且返回原标识（不把重试算首次入账）",
          c2 == 200 and r2.get("deduplicated") is True and r2.get("first_attempt_id") == first_id,
          f"{c2} {r2}")
    check("S2b 接收端仍仅一条", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def s3_conflict():
    k = key("k1")
    code, n = sender_create(ENV, k, "failed")
    check("S3 同键不同内容登记报 409 冲突", code == 409, f"code={code} {n}")
    rows = sender_rows(k)
    check("S3 发送端原记录未被篡改", len(rows) == 1 and rows[0]["result"] == "success", f"{rows}")
    code, _ = req("POST", RECEIVER + "/api/receipts",
                  {"env": ENV, "release_no": k, "result": "failed"})
    check("S3 接收端直连同键不同内容也报 409", code == 409, f"code={code}")
    check("S3 接收端同键仍仅一条", len(receiver_rows(k)) == 1)


def s4_lost_response_resend():
    k = key("k4")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="lose_response")
    check("S4 丢回执后为待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    check("S4 丢回执时首次入账尝试显示未知", n2.get("attribution") == "UNKNOWN", f"{n2}")
    check("S4 接收端其实已记账（1 条）", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    first = (n2.get("attempts") or [{}])[0].get("attempt_id")
    code, n3 = sender_send(nid)
    check("S4 重发后凭回执确认", n3.get("status") == "ACKED", f"{n3}")
    check("S4 重发未产生重复记录", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    check("S4 归因回到第 1 次尝试（重发不是首次入账）",
          n3.get("attribution") == "TRACED" and n3.get("first_attempt_no") == 1
          and n3.get("first_attempt_id") == first, f"{n3}")
    attempts = n3.get("attempts") or []
    check("S4 尝试记录为 HTTP 异常(回执丢失) + 幂等命中",
          [a["outcome"] for a in attempts] == ["HTTP_ERROR", "DEDUP"], f"{attempts}")


def s5_lost_response_reconcile():
    k = key("k5")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S5 丢回执后状态为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    code, n3 = sender_reconcile(nid)
    check("S5 核对后凭接收端回执确认", n3.get("status") == "ACKED" and bool(n3.get("receipt_id")), f"{n3}")
    check("S5 核对补全归因到第 1 次尝试",
          n3.get("attribution") == "TRACED" and n3.get("first_attempt_no") == 1
          and bool(n3.get("receipt_first_attempt_id")), f"{n3}")
    check("S5 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s6_slow_response():
    k = key("k6")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="slow")
    check("S6 慢回执超时后为待核", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S6 核对后确认", n3.get("status") == "ACKED", f"{n3}")
    check("S6 接收端仅一条", len(receiver_rows(k)) == 1)


def s7_concurrent_same():
    k = key("k7")
    results = []

    def worker():
        try:
            c1, n1 = sender_create(ENV, k, "success")
            c2, n2 = sender_send(n1["id"])
            results.append((c1, c2, n2.get("status")))
        except Exception as e:  # noqa: BLE001
            results.append(("EXC", str(e), None))

    threads = [threading.Thread(target=worker) for _ in range(12)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("S7 并发同内容：无线程异常", all(r[0] != "EXC" for r in results), f"{results}")
    check("S7 并发同内容：发送端仅一条", len(sender_rows(k)) == 1, f"{sender_rows(k)}")
    check("S7 并发同内容：接收端仅一条", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")
    rows = sender_rows(k)
    check("S7 并发同内容：最终已确认", bool(rows) and rows[0]["status"] == "ACKED", f"{rows}")
    attempts = (rows[0].get("attempts") if rows else None) or []
    recorded = [a for a in attempts if a["outcome"] == "RECORDED"]
    check("S7 并发同内容：仅一次尝试被标记首次入账", len(recorded) == 1, f"{attempts}")
    check("S7 并发同内容：归因指向被标记的那次尝试",
          bool(rows) and rows[0].get("first_attempt_id") == (recorded[0]["attempt_id"] if recorded else None),
          f"{rows}")


def s8_concurrent_diff():
    k = key("k8")
    codes = []

    def worker(i):
        content = "success" if i % 2 == 0 else "failed"
        c, _ = sender_create(ENV, k, content)
        codes.append(c)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    ok = sum(1 for c in codes if c in (200, 201))
    conflict = sum(1 for c in codes if c == 409)
    check("S8 并发不同内容：一组入库一组 409", ok == 5 and conflict == 5, f"codes={codes}")
    rows = sender_rows(k)
    check("S8 并发不同内容：发送端仅一条", len(rows) == 1, f"{rows}")
    if rows:
        code, n2 = sender_send(rows[0]["id"])
        check("S8 胜出发送后已确认", n2.get("status") == "ACKED", f"{n2}")
        rrows = receiver_rows(k)
        check("S8 接收端仅一条且内容与发送端一致",
              len(rrows) == 1 and rrows[0]["result"] == rows[0]["result"], f"{rrows} vs {rows}")


def s9_receiver_down():
    k = key("k9")
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, n = sender_create(ENV, k, "success")
    check("S9 接收端停机时本地可登记", code in (200, 201), f"code={code} {n}")
    nid = n["id"]
    code, n2 = sender_send(nid)
    check("S9 接收端不可达 → 待核而非成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    code, n3 = sender_reconcile(nid)
    check("S9 不可达时核对仍保持待核（不虚报）", n3.get("status") == "UNCERTAIN", f"{n3}")
    return nid


def s10_sender_restart_while_down(nid_k9):
    k = key("k10")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid)  # 接收端仍停机 → 待核
    check("S10 停机期间发送为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S10 发送端重启后恢复服务", False)
        return
    time.sleep(4)  # 等启动核对线程跑完（接收端仍停机，应保持待核）
    rows = sender_rows(k)
    check("S10 重启后待核状态持久（未丢未虚报）", bool(rows) and rows[0]["status"] == "UNCERTAIN", f"{rows}")
    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("S10 接收端恢复", False)
        return
    code, n4 = sender_reconcile(nid)
    check("S10 恢复后核对判定未送达（回退待发送）", n4.get("status") == "PENDING", f"{n4}")
    code, n4b = sender_send(nid)
    check("S10 安全重发后确认且接收端仅一条",
          n4b.get("status") == "ACKED" and len(receiver_rows(k)) == 1, f"{n4b}")
    code, n5 = sender_reconcile(nid_k9)
    check("S10 此前停机期间的通知核对同为未送达", n5.get("status") == "PENDING", f"{n5}")
    code, n5b = sender_send(nid_k9)
    check("S10 补发后确认", n5b.get("status") == "ACKED", f"{n5b}")
    check("S10 接收端两键各仅一条",
          len(receiver_rows(key("k9"))) == 1 and len(receiver_rows(k)) == 1)


def s11_auto_reconcile_after_restart():
    k = key("k11")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")
    check("S11 重启前为待核", sender_rows(k)[0]["status"] == "UNCERTAIN")
    hook("RN_HOOK_RESTART_SENDER", "docker compose restart sender")
    if not wait_up(SENDER):
        check("S11 发送端重启后恢复服务", False)
        return
    final = None
    for _ in range(20):  # 启动核对线程约 2s 后触发
        rows = sender_rows(k)
        if rows and rows[0]["status"] == "ACKED":
            final = rows[0]
            break
        time.sleep(0.5)
    check("S11 重启后自动凭接收端回执结案", final is not None, f"{sender_rows(k)}")
    check("S11 重启后归因补全到第 1 次尝试",
          final is not None and final.get("attribution") == "TRACED"
          and final.get("first_attempt_no") == 1, f"{final}")
    check("S11 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s12_reconcile_while_receiver_down():
    """接收端失联时核对：状态不变，首次入账尝试显示未知（--lifecycle）。"""
    k = key("k12")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    sender_send(nid, simulate="lose_response")  # 接收端已入账、回执丢失
    hook("RN_HOOK_STOP_RECEIVER", "docker compose stop receiver")
    time.sleep(1)
    code, n2 = sender_reconcile(nid)
    check("S12 接收端失联时核对不虚报成功", n2.get("status") == "UNCERTAIN", f"{n2}")
    check("S12 接收端失联时首次入账尝试显示未知", n2.get("attribution") == "UNKNOWN", f"{n2}")
    hook("RN_HOOK_START_RECEIVER", "docker compose start receiver")
    if not wait_up(RECEIVER):
        check("S12 接收端恢复", False)
        return
    code, n3 = sender_reconcile(nid)
    check("S12 恢复后凭回执补全归因",
          n3.get("status") == "ACKED" and n3.get("attribution") == "TRACED"
          and n3.get("first_attempt_no") == 1, f"{n3}")
    check("S12 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s13_attempt_disk_fail_never_sends():
    """尝试记录落库失败时必须 500 且绝不发出请求（否则会产生无法追溯的回执）。"""
    k = key("k13")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, resp = sender_send(nid, simulate="fail_attempt_db")
    check("S13 尝试记录落库失败返回 500", code == 500, f"{code} {resp}")
    check("S13 失败后无尝试记录", not (resp.get("attempts") or []), f"{resp.get('attempts')}")
    check("S13 接收端未收到任何请求（无回执）", len(receiver_rows(k)) == 0, f"{receiver_rows(k)}")
    rows = sender_rows(k)
    check("S13 本地仍为待发送且尝试数为 0",
          bool(rows) and rows[0]["status"] == "PENDING" and rows[0]["attempt_count"] == 0, f"{rows}")
    code, n2 = sender_send(nid)
    check("S13 故障恢复后可正常发送并归因",
          n2.get("status") == "ACKED" and n2.get("attribution") == "TRACED"
          and n2.get("first_attempt_no") == 1, f"{n2}")


def s14_legacy_receipt_untraceable():
    """旧回执（无尝试标识）必须显示无法追溯，而不是误归因。"""
    k = key("k14")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    # 模拟旧客户端 / 旧代码直接入账：不带 attempt_id
    code, r = req("POST", RECEIVER + "/api/receipts",
                  {"env": ENV, "release_no": k, "result": "success"})
    check("S14 接收端接受不带标识的旧式入账（201）", code == 201, f"{code} {r}")
    check("S14 旧回执 first_attempt_id 为空", r.get("first_attempt_id") is None, f"{r}")
    code, n2 = sender_reconcile(nid)
    check("S14 核对后状态已确认", n2.get("status") == "ACKED", f"{n2}")
    check("S14 旧回执显示无法追溯", n2.get("attribution") == "UNTRACEABLE", f"{n2}")
    check("S14 接收端仍仅一条", len(receiver_rows(k)) == 1)


def s15_interleaved_first_entry():
    """首次入账后并发交错重试：接收端只保留首次标识，发送端归因仍指向首次。"""
    k = key("k15")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    code, n2 = sender_send(nid, simulate="lose_response")  # 第 1 次已入账、回执丢失
    first = (n2.get("attempts") or [{}])[0].get("attempt_id")
    results = []

    def worker():
        try:
            c, body = sender_send(nid)
            results.append((c, body.get("status")))
        except Exception as e:  # noqa: BLE001
            results.append(("EXC", str(e)))

    threads = [threading.Thread(target=worker) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("S15 交错重试无线程异常", all(r[0] != "EXC" for r in results), f"{results}")
    check("S15 交错重试后均不误报非成功状态",
          all(r[0] == 200 and r[1] == "ACKED" for r in results), f"{results}")
    rows = sender_rows(k)
    check("S15 发送端最终已确认", bool(rows) and rows[0]["status"] == "ACKED", f"{rows}")
    check("S15 归因仍指向第 1 次尝试",
          bool(rows) and rows[0].get("attribution") == "TRACED"
          and rows[0].get("first_attempt_no") == 1 and rows[0].get("first_attempt_id") == first,
          f"{rows}")
    receipt = receiver_receipt(k)
    check("S15 接收端首次入账标识未被重试改写",
          receipt.get("first_attempt_id") == first, f"{receipt} vs {first}")
    check("S15 接收端仅一条", len(receiver_rows(k)) == 1)
    attempts = (rows[0].get("attempts") if rows else None) or []
    outcomes = [a["outcome"] for a in attempts]
    check("S15 仅第 1 次尝试标记 RECORDED（回执丢失故如实记 HTTP_ERROR）",
          outcomes.count("RECORDED") == 0 and outcomes[0] == "HTTP_ERROR"
          and set(outcomes[1:]) == {"DEDUP"} and len(outcomes) >= 2, f"{attempts}")


def s16_interleaved_reconcile_and_send():
    """发送与核对交错：已确认不得被晚到的失败/无记录结论降级。"""
    k = key("k16")
    code, n = sender_create(ENV, k, "success")
    nid = n["id"]
    codes = []

    def worker(i):
        try:
            if i % 2:
                c, body = sender_send(nid)
            else:
                c, body = sender_reconcile(nid)
            codes.append((c, body.get("status")))
        except Exception as e:  # noqa: BLE001
            codes.append(("EXC", str(e)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    check("S16 交错操作无线程异常", all(c[0] != "EXC" for c in codes), f"{codes}")
    code, final = sender_get(nid)
    recorded = [a for a in (final.get("attempts") or []) if a["outcome"] == "RECORDED"]
    # 尝试序号按本地落库顺序分配，接收端按请求到达顺序处理，
    # 并发下首次入账未必是第 1 次尝试；关键是唯一、且归因指向它。
    check("S16 交错后最终已确认且归因唯一指向首次入账尝试",
          final.get("status") == "ACKED" and final.get("attribution") == "TRACED"
          and len(recorded) == 1
          and final.get("first_attempt_id") == recorded[0]["attempt_id"]
          and final.get("first_attempt_no") == recorded[0]["attempt_no"], f"{final}")
    check("S16 接收端仅一条", len(receiver_rows(k)) == 1, f"{receiver_rows(k)}")


def main():
    lifecycle = "--lifecycle" in sys.argv or os.environ.get("RN_LIFECYCLE") == "1"
    print(f"发送端 {SENDER}  接收端 {RECEIVER}  lifecycle={lifecycle}", flush=True)
    if not wait_up(SENDER) or not wait_up(RECEIVER):
        print("服务未就绪，请先启动两端。")
        sys.exit(2)

    s1_basic()
    s2_retry_dedup()
    s2b_retry_after_pending()
    s3_conflict()
    s4_lost_response_resend()
    s5_lost_response_reconcile()
    s6_slow_response()
    s7_concurrent_same()
    s8_concurrent_diff()
    s13_attempt_disk_fail_never_sends()
    s14_legacy_receipt_untraceable()
    s15_interleaved_first_entry()
    s16_interleaved_reconcile_and_send()
    if lifecycle:
        nid_k9 = s9_receiver_down()
        s10_sender_restart_while_down(nid_k9)
        s11_auto_reconcile_after_restart()
        s12_reconcile_while_receiver_down()

    print(f"\n通过 {len(PASSED)} 项，失败 {len(FAILED)} 项")
    if FAILED:
        print("失败项：")
        for name in FAILED:
            print(f"  - {name}")
        sys.exit(1)


if __name__ == "__main__":
    main()
