# 职业教学任务运营服务

这是一个面向职业院校教务团队、授课教师和课程管理员的 Python 后端服务，用于管理课程任务模板、学员提交、执行队列、教师工作者、结果版本和运营干预。服务保留登录、角色权限、会话、审计和配额等基础能力，所有业务状态与审计事件写入本地 SQLite 数据库，适合在单个应用容器中离线运行。

## 运行环境

- Python 3.11
- SQLite 3（由 Python 标准库提供）
- FastAPI 与 Uvicorn

## 安装

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e ".[dev]"
```

默认数据库位于 `./data/compute-operations.db`，可复制 `.env.example` 并设置 `TOWNSHIP_DATABASE_PATH` 指向其他本地路径。

## 初始化与启动

```bash
python -m app.cli init-db
python -m app.cli check-db
uvicorn app.main:app --host 0.0.0.0 --port 8432
```

健康检查：

```bash
curl -sS http://127.0.0.1:8432/api/system/health
```

课程任务运营接口使用 `/api/compute` 前缀，身份、角色、审计和系统接口分别位于 `/api/auth`、`/api/roles`、`/api/audit` 与 `/api/system`。

## 成果文件清单与生命周期

学生作业附带的运行日志、仿真文件等成果以**清单**形式随工作者完成回执提交，服务核对后与成绩版本同事务落库，避免只看结果正文的清理任务误删仍被公开成绩引用的资料。

- **提交清单**：`POST /api/compute/tasks/{task_id}/complete` 的 `artifacts` 数组逐项声明 `worker_path`（工作者提交路径）、`filename`、`size_bytes`、`content_sha256`（摘要）、`purpose`（用途）。内容可内联 `content_base64`，或先用 `POST /api/compute/artifacts/staging` 暂存再以 `staging_key` 引用。携带成果的回执必须提供 `receipt_key`。
- **先核对后落库**：服务对每项内容重算 sha256 与大小；任一摘要或大小不符，整个回执回滚，任务保持 `running`、不会转为成功，且不留半成品文件。
- **回执幂等**：相同 `receipt_key` 的重复回执只产生同一份清单和结果版本；回执内容变化或工作者不一致会被拒绝。
- **生命周期状态**：成果随成绩版本处于 `candidate`（候选）、`published`（已发布）、`withdrawn`（已撤回）之一；暂存区文件是独立的 `temporary` 临时文件。各状态保留期由环境变量配置（见下）。
- **清理规则**：`POST /api/compute/retention/purge-artifacts`（可 `dry_run=true`）删除过期暂存与过期且无引用的成果。**已发布成绩永久保留**；候选成果若仍是任务当前成绩版本则强制保留；撤回版本在撤回保留期后可清。物理文件按内容寻址去重，仅当不存在任何有效引用时才删除；清理保留成果墓碑（下载返回 410）。
- **下载鉴权**：`GET /api/compute/artifacts/{id}/download` 仅放行“仍在保留期且有权限”的成果（提交人本人、项目授权教师/观察员、管理员；观察员只能看已发布版本；已发布不受保留期限制）。
- **访问说明**：`GET /api/compute/artifacts/{id}/access?requester=...` 返回该文件所属任务、成绩版本、是否当前版本、生命周期/发布状态、保留到期时间，以及 `allowed`、`decision_code` 和人类可读的允许或拒绝原因。
- **成绩发布/撤回**：`POST /api/compute/tasks/{id}/results/{version}/publish|withdraw`；授权教师用 `PUT /api/compute/project-members` 维护。

成果物理存储默认在数据库同级 `artifacts/` 目录（内容寻址），可用 `TOWNSHIP_ARTIFACT_STORE_PATH` 覆盖：

| 环境变量 | 默认 | 含义 |
| --- | --- | --- |
| `TOWNSHIP_ARTIFACT_TEMPORARY_RETENTION_HOURS` | 24 | 暂存临时文件保留小时数（接口可按 TTL 缩短） |
| `TOWNSHIP_ARTIFACT_CANDIDATE_RETENTION_DAYS` | 30 | 候选成绩成果保留天数 |
| `TOWNSHIP_ARTIFACT_PUBLISHED_RETENTION_DAYS` | 3650 | 已发布成绩成果保留天数（公开成绩在此期间强制保留） |
| `TOWNSHIP_ARTIFACT_WITHDRAWN_RETENTION_DAYS` | 30 | 撤回成绩成果保留天数 |

命令行清理：

```bash
python -m app.cli purge-artifacts --dry-run
python -m app.cli purge-artifacts
```

## 测试与编译检查

```bash
python -m pytest
python -m compileall -q app tests
```

本地冒烟命令：

```bash
python -m app.cli smoke
python -m app.cli compute-demo
```

## 目录结构

```text
app/compute/       任务模板、配额、提交、领取、回执和人工干预
app/api/            登录、角色、审计和系统管理接口
app/core/           时钟、安全、异常和分页能力
app/repositories/   SQLite 查询与事务封装
app/services/       身份、审计和后台任务服务
app/database.py     SQLite 连接、事务、表结构和权限初始化
tests/              领域、接口、调度和身份回归测试
tools/              本地维护脚本
```

## 数据一致性

SQLite 连接启用外键、WAL 和忙等待策略。提交、领取、回执和人工干预在即时事务中完成；租约、配额与结果版本使用可注入时钟，便于复现跨日和恢复边界。会话令牌只保存摘要，审计与人工干预记录不会写入明文密码或令牌。
