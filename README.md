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

## 成果文件清单与生命周期

学生作业附带的运行日志与仿真文件不再只靠清理任务扫描正文，而是先登记、随成绩版本落库，并按状态执行差异化保留：

1. **上传登记**：工作者把单个文件以 `PUT /api/compute/artifacts/uploads/{upload_id}` 写入服务端受控暂存区，对象名由服务端生成；服务端实测并返回逻辑路径、大小、sha256 摘要和用途，暂存项按临时保留期（默认 24 小时）过期。
2. **先核对清单再落库**：完成回执 `POST /api/compute/tasks/{id}/complete` 携带 `artifacts` 清单（上传标识、路径、文件名、用途、大小、摘要）和可选 `receipt_key`。服务在同一即时事务内独立复算每份文件的大小与摘要，任一不符都回滚，任务不会转为成功；全部通过后清单才与成绩版本一起落库，物理文件按 sha256 内容寻址存储并去重。
3. **回执幂等**：同一 `(task_id, receipt_key)` 的重复回执只返回既有版本，不会产生第二份成绩版本或清单。
4. **版本生命周期**：成绩版本与清单为 `candidate`（候选）→ `published`（已发布）→ `withdrawn`（撤回）。发布把公开成绩引用指向该版本（独立于最新提交版本）；发布新版本会把旧发布版本级联为“被替代撤回”；撤回解除公开引用。
5. **差异化清理**：`POST /api/compute/retention/cleanup-artifacts` 分别处理临时暂存（短保留期）、候选（默认 7 天）、撤回（默认 30 天）和已发布（默认长期）。**被公开成绩引用（`published_result_version`）的内容一律保留**；物理 blob 在无任何清单引用后才删除，物理删除放在事务提交之后。
6. **可解释的下载授权**：
   - `GET .../artifacts/{filename}/access?requested_by=...` 只返回判定：文件属于哪个任务的哪个成绩版本、生命周期状态、保留截止时间、是否被公开成绩引用，以及允许或拒绝的具体原因，不返回文件内容。
   - `GET .../artifacts/{filename}/download?requested_by=...` 强制执行同一规则：仅在保留期内且身份有权限（项目任课教师/管理员，撤回版本仅管理员在留存期内审计）时返回字节，并再次校验 blob 摘要。

项目教师/管理员通过 `POST /api/compute/project-members` 授权。相关保留期与存储目录由 `TOWNSHIP_ARTIFACT_*` 环境变量配置，默认存储在数据库同级的 `artifacts/` 目录。

