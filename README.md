# 部署完成通知 · 幂等送达与首次入账归因

发布值班员通过网页向审计端发送「部署完成通知」。网络超时时无法判断对方是否已记账，
盲目重发又会产生重复记录。本系统用 **FastAPI + SQLite** 实现两端（各自独立数据库），
以「**幂等键 + 回执核对 + 尝试标识归因**」解决该问题：

- 相同「环境 + 发布号」的同内容重试，两端都只记一次；
- 同键不同内容 → **409 冲突**；
- 丢回执 / 接收端不可达 / 发送端重启 → 状态保持 **待核**，**绝不虚报成功**，
  恢复后凭接收端回执核对定案（已入账→已确认；未入账→回退待发送，可安全重发）；
- 每次发送尝试单独落库并带唯一 `attempt_id`；接收端**首次入账**时保存该标识，
  同内容重试始终返回**原标识**，因此能回答「**哪次发送让审计端首次入账**」，
  而不会把后来的重试算作首次入账。

## 目录结构

```
├── docker-compose.yml      # 一键启动两端
├── sender/                 # 发送端（值班网页 + 发送/核对/归因逻辑），端口 8000
│   ├── Dockerfile
│   └── app/{main.py, static/index.html}
├── receiver/               # 审计接收端（演示用，含故障注入），端口 8001
│   ├── Dockerfile
│   └── app/{main.py, static/index.html}
└── tests/
    ├── selftest.py         # 80 项断言的关键情形自测（仅标准库）
    └── run_local.sh        # 无 Docker 时的本地进程运行器
```

## 快速开始（Docker Compose）

```bash
docker compose up --build
```

- 发送端网页：http://localhost:8000 （填写环境、发布号、结果，提交并发送；
  列表「首次入账尝试」列直接显示归因结果，点「尝试」列的「明细」可看每次尝试）
- 接收端台账：http://localhost:8001 （实时查看已入账回执及其 `first_attempt_id`）

两端各持有一个独立 SQLite 库（compose 命名卷 `sender_data` / `receiver_data`）。

无 Docker 的本地环境（需 Python 3.11+ 及 fastapi/uvicorn/httpx）：

```bash
tests/run_local.sh start-all     # 两个独立进程 + 两个独立 db 文件
tests/run_local.sh stop-all
```

## 尝试记录与首次入账归因

### 发送端：`attempts` 表（每次发送一行）

| 字段 | 含义 |
|---|---|
| `attempt_no` | 本地第几次发送（1 开始，按本地落库顺序） |
| `attempt_id` | 本次尝试的唯一标识（随请求发给接收端） |
| `outcome` | `IN_FLIGHT` / `RECORDED`（本次首次入账）/ `DEDUP`（接收端已有同内容）/ `CONFLICT` / `CONTENT_MISMATCH` / `HTTP_ERROR` / `UNREACHABLE`（超时、连接失败）/ `INTERRUPTED`（发送端重启时仍在进行，结果未知） |

**落库先于请求**：`attempts` 行与「待核」状态在同一 `BEGIN IMMEDIATE` 事务内写入，
写入失败即返回 500 且**绝不向接收端发出请求** —— 否则接收端可能已入账，而发送端
没有任何尝试记录可用于归因，会产生无法追溯的回执。

重启时，遗留在 `IN_FLIGHT` 的尝试被标记为 `INTERRUPTED`（结果未知，不臆测），
随后启动核对线程凭接收端回执补全归因。

### 接收端：`receipts.first_attempt_id`

- 首次入账（`INSERT`）时保存请求里的 `attempt_id`；
- 同内容重试命中 `UNIQUE(env, release_no)` → 返回既有回执（200, `deduplicated`），
  **`first_attempt_id` 保持首次值，绝不改写**；
- 旧回执 / 旧客户端未带标识 → `first_attempt_id` 为 `NULL`。

### 归因状态（发送端 API 返回 `attribution`）

| 值 | 含义 | 网页显示 |
|---|---|---|
| `TRACED` | 回执的 `first_attempt_id` 命中本端某次尝试 | 第 N 次尝试（含尝试总数） |
| `UNTRACEABLE` | 已收到回执，但回执没有尝试标识（旧回执/旧客户端） | 无法追溯（回执无尝试标识） |
| `FOREIGN` | 回执标识不属于本端任何尝试 | 无法追溯（标识不属于本端尝试） |
| `UNKNOWN` | 待核 / 接收端失联，尚不能确定 | 未知（待核对/接收端失联） |
| `NONE` | 尚未发送，无回执 | — |

归因**只认接收端回执**：本地尝试记录只用于把 `first_attempt_id` 翻译成「第几次尝试」，
不用本地的成功/失败猜测首次入账。

## 状态机（发送端）

| 状态 | 含义 | 进入条件 |
|---|---|---|
| 待发送 PENDING | 本地已登记，未发送或已确认对方未记账 | 登记成功；核对时接收端明确无此记录 |
| 待核 UNCERTAIN | 已发出但回执缺失/对方不可达，送达与否未知 | 发送前落库；超时、连接失败、对方 5xx |
| 已确认 ACKED | 收到接收端内容一致的回执 | 发送响应 2xx 且内容哈希一致；核对命中 |
| 冲突 CONFLICT | 同环境同发布号在接收端已存在不同内容 | 发送/核对时发现内容不一致（需人工处理） |

关键约束：**只有接收端回执能把记录变为「已确认」**。发送动作会先把「待核」落库再发 HTTP，
即使进程在发送途中崩溃，重启后也知道必须核对，而不是误报成功或误当未发。
已确认是终态：并发/迟到的失败回执只记入该次尝试明细，不把已确认降级。

## 演示场景

1. **正常送达**：网页填写 `prod / R2026-001 / 成功` → 提交 → 状态「已确认」，
   首次入账尝试显示「第 1 次尝试（1 次中）」，接收端台账出现一条并带相同标识。
2. **重复提交**：再次提交相同内容 → 发送端幂等去重、接收端仍只有一条，归因不变。
3. **内容冲突**：同环境同发布号改选「失败」→ 提交即报 409 冲突，原记录不被篡改。
4. **入库后丢回执**：勾选「模拟接收端故障」再提交 → 状态变「待核」、首次入账尝试「未知」
   （接收端其实已入账）；点「重试」→ 接收端幂等返回**原标识**，状态转「已确认」，
   归因仍为第 1 次尝试（第 2 次只是 DEDUP），接收端仍只有一条。
5. **接收端不可用**：`docker compose stop receiver` → 发送/核对均保持「待核」
   （不虚报成功，归因显示未知）；`docker compose start receiver` 后点「核对」→
   若对方无记录则回退「待发送」，可安全重发。
6. **发送端重启**：`docker compose restart sender` → 状态与尝试记录持久不丢；
   启动后自动核对所有待核记录，凭接收端回执结案并补全归因（接收端不可达时保持待核）。
7. **旧回执**：接收端已有无标识的历史回执 → 发送端核对后置「已确认」，
   首次入账尝试显示「无法追溯（回执无尝试标识）」。

## API 摘要

发送端（:8000）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/notifications` | 登记（同键同内容幂等，同键不同内容 409） |
| GET | `/api/notifications` | 列表（含 `attempts`、`attribution`、`first_attempt_no`） |
| GET | `/api/notifications/{id}` | 单条详情 |
| POST | `/api/notifications/{id}/send[?simulate=lose_response\|slow\|fail_attempt_db]` | 发送/重发；`fail_attempt_db` 为演示用：模拟尝试落库失败（不发请求） |
| POST | `/api/notifications/{id}/reconcile` | 凭接收端回执核对并补全归因 |
| POST | `/api/reconcile-all` | 核对全部待核/待发送（跳过有请求在途的记录） |

接收端（:8001）：

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/receipts` | 幂等入账；body 可带 `attempt_id`（首次入账保存为 `first_attempt_id`）、`"simulate":"lose_response"`（入库后丢响应）或 `"slow"`（慢回执） |
| GET | `/api/receipts?env=..&release_no=..` | 查询单条回执（404 表示无记录） |
| GET | `/api/receipts` | 台账列表 |

## 幂等与并发设计

- 两端均以 `UNIQUE(env, release_no)` 为幂等键，内容身份为 `{env, release_no, result}` 的 SHA-256；
- 写入路径 `BEGIN IMMEDIATE` 事务内「先查后插」，配合 WAL 与 busy_timeout：
  并发下同键只落一行，后到者走幂等返回或 409；
- 重发安全：接收端对同内容重试返回既有回执（200, `deduplicated`），不新增记录，
  也不改写 `first_attempt_id`；并发交错下首次入账尝试唯一，归因不会漂移到重试；
- 尝试落库失败→500 且不发请求（无「无法追溯」的回执产生）；接收端写失败→5xx→发送端保持待核；
  拿到回执但本地更新失败→仍为待核/可核对，尝试明细保留；
- 已确认是终态：并发的发送/核对交错时，晚到的超时/无记录结论不会把已确认降级。

## 自测

```bash
# Docker 模式（先 docker compose up --build）
python3 tests/selftest.py --lifecycle        # 含停接收端/重启发送端场景（钩子默认 docker compose）

# 无 Docker 本地模式
tests/run_local.sh test
```

覆盖 80 项断言：正常送达与归因、同内容重试去重（不改写首次标识）、同键冲突、
丢回执后重发/核对并补全归因、慢回执超时、12/10 线程并发、尝试落库失败绝不发请求、
旧回执无法追溯、首次入账后交错重试、发送与核对交错不降级、接收端停机不虚报、
发送端重启后待核持久与自动补核、接收端失联时归因显示未知。
