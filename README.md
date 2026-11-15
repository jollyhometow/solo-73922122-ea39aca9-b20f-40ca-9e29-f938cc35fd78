# Corpus Contamination Checker

新闻语料近重复核查服务：在模型评测前找出训练集与测试集之间的近重复文章，
帮助语料工程师决定撤下哪些测试材料。另附**编码修复 API**：针对抓取时被
Latin-1 / Windows-1252 误解码的局部 UTF-8 片段（mojibake），先预览候选、
再原子确认修复，避免整篇转码伤及正常文字。

- **FastAPI + SQLite**，两个 API 进程共享同一个数据库文件（WAL）
- 以**原文连续 5 字符 shingle 集合的 Jaccard 相似度 ≥ 0.8** 连边，按连通分量分组
- 相似边与分组**持久化**，按语料代次（generation）发布；查询只读已发布结果，不做现场全量比对
- 仅当已发布结果**覆盖当前全部已提交语料**时才给出核查结论（200），否则返回 **202 + 待核查状态**

## 快速开始

### Docker Compose（推荐）

```bash
docker compose up --build
```

- API 进程 1: <http://localhost:8001/docs> （Swagger UI）
- API 进程 2: <http://localhost:8002/docs>
- 两个进程共享 `corpus-data` 卷里的同一个 SQLite 文件
- `seeder` 一次性容器会在两个 API 健康后自动载入演示语料（含桥接文章），随后退出

### 本地（无 Docker）

```bash
pip install -r requirements.txt
scripts/local_dev.sh start        # api1 -> :8001, api2 -> :8002，共库 /tmp/corpus-local/corpus.db
scripts/local_dev.sh stop
```

## 演示场景

演示语料（`scripts/demo.py`，确定性生成，内置自校验）：

| 文章 | 归属 | 说明 |
|---|---|---|
| `train-direct-1` / `test-direct-1` | train / test | 直接近重复对（相似度 ≈ 0.95） |
| `train-bridge-a` | train | 与桥接文章近重复（≈ 0.86） |
| `test-bridge` | test | **桥接文章**：同时桥接 train 侧与 test 侧 |
| `test-bridge-b` | test | 与桥接文章近重复（≈ 0.87），但与 `train-bridge-a` 仅 ≈ 0.74（不连边） |
| `train-clean-1` / `test-clean-1` | train / test | 干净对照 |

```bash
# 1) 载入演示语料并查看两个跨集合污染组
python3 scripts/demo.py seed --base http://localhost:8001

# 2) 桥接文章：删除 test-bridge -> 污染组拆开；补回 -> 组重新形成
python3 scripts/demo.py bridge --base http://localhost:8002

# 3) 修改中暂停比对：比对进行中（COMPARE_DELAY_MS=3000）连续提交两批，
#    期间查询得到 202；旧代次的计算结果被丢弃，最终只发布最新代次
python3 scripts/demo.py concurrent --base http://localhost:8001

# 4) 编码修复：对误解码片段预览两种逆转换候选，确认后原子修复并重新核查
python3 scripts/demo.py encoding --base http://localhost:8002

# 5) 重启：提交一批后立即重启两个进程，核查在重启后自动补完，无需重新提交
python3 scripts/demo.py restart --base http://localhost:8001 \
    --restart-cmd "docker compose restart api1 api2"
# 本地模式：--restart-cmd "scripts/local_dev.sh restart"

# 或一次跑完：demo.py all --base ... --restart-cmd ...
```

## API

| 方法/路径 | 说明 |
|---|---|
| `POST /articles/batch` | 整批补录/更正/删除：`{"upserts": [{id, side, text}], "deletes": [id]}`。**单事务原子生效**，任一失败整批回滚 |
| `GET /articles` | 当前已提交语料列表（含每篇的 `updated_generation`） |
| `POST /encoding-fix/preview` | 编码修复预览：至多 5 篇、每篇至多 6 个互不重叠半开区间；逐段给出 Latin-1 / Windows-1252 两种逆转换的有效候选，无法转换的说明原因（只读） |
| `POST /encoding-fix/confirm` | 编码修复确认：单事务内按当前原文重算并核对版本/区间/候选/长度，全部通过才整批提交；语料代次只推进一次并重新核查 |
| `GET /contamination` | 核查结论。`200`：跨集合污染组、成员、直接相似边；`202`：待核查（附 current/published 代次） |
| `GET /status` | 实例、代次、语料计数、后台作业状态 |
| `GET /docs` | Swagger UI |

校验规则：文章 ID 1–128 字符；`side ∈ {train, test}`；原文 5–2000 字符；
全库最多 **300** 篇（超出整批拒绝，409）；同批内 ID 不得重复、不得同时 upsert 与 delete。

响应示例（200）：

```json
{
  "status": "ok",
  "generation": 3,
  "contaminated": true,
  "contamination_group_count": 1,
  "same_set_group_count": 0,
  "groups": [
    {
      "group_id": 1,
      "size": 3,
      "train_members": ["train-bridge-a"],
      "test_members": ["test-bridge", "test-bridge-b"],
      "members": [{"id": "...", "side": "test", "text": "..."}],
      "edges": [
        {"a": "test-bridge", "b": "train-bridge-a", "similarity": 0.858},
        {"a": "test-bridge", "b": "test-bridge-b", "similarity": 0.868}
      ]
    }
  ]
}
```

## 设计：为什么这些需求同时成立

**语料代次（generation）** 是所有一致性的锚点：

```
提交批次  ──事务──>  articles 更新 + current_generation+1 + 比对作业入队
后台worker ──租约认领作业──> 一致性快照 ──> 计算边/组 ──> 发布事务
查询      ──> 仅当 published_generation == current_generation 才返回 200
```

- **相似边/分组持久化，查询不现场比对**：worker 把结果写入 `edges` /
  `article_groups`（按代次），`GET /contamination` 只读已发布数据。
- **两进程共库、比对时可提交**：SQLite WAL + 短 `BEGIN IMMEDIATE` 写事务；
  比对计算不持有写锁。作业存于 `compute_jobs`，带租约认领——任一时刻只有一个
  进程真正计算，另一个照常服务；进程崩溃后租约到期由存活进程接管。
- **旧计算不覆盖新修改**：发布事务内复查 `current_generation` 未变才落库，
  否则丢弃该次结果（快照时发现代次已前进也会直接丢弃作业）。
- **已发布结果不混版**：结果指针 `published_generation` 与数据在同一事务内
  原子切换；读取也在单个读事务内完成，永远只看到一个完整代次。
- **不把旧组配上新原文 / 不误报无污染**：任何提交都会使
  `current_generation > published_generation`，此时查询一律 202，
  直到覆盖最新语料的核查发布。
- **删除桥接文章即拆组**：分组是按当前语料快照重算的连通分量，桥接节点
  消失后原组自然拆分（见演示 2）。
- **整批原子性**：格式校验（pydantic）→ 单事务应用 + 300 篇上限检查，
  任一失败整体回滚。
- **重启自动补完**：启动时若 `current_generation > published_generation`
  自动补建待比对作业；作业租约机制保证崩溃进程未完成的任务被接管。

## 设计：编码修复（mojibake）如何不越修越坏

抓取入库的稿件偶尔混有"UTF-8 字节被当作 Latin-1 / Windows-1252 解码"的
局部乱码（如 `cafÃ©`、`â‚¬`）。整篇转码会把正常文字也卷进去，所以修复
以**片段**为单位，并分预览 / 确认两步：

```
GET /articles            -> 取每篇的 updated_generation（每篇各自的版本号）
POST /encoding-fix/preview -> 逐段试两种逆转换，列出有效候选 / 失败原因
POST /encoding-fix/confirm -> 选定各段候选，单事务核对后整批生效
```

- **逆转换**：候选 = `segment.encode(误解码字符集).decode("utf-8")`。
  编码失败（含该字符集无法表示的字符）、解码失败（字节不是合法 UTF-8）
  或转换后文本不变，都不构成有效候选，预览会给出具体原因——正常文字
  （如本来就正确的 `é`、纯 ASCII）因此永远不会被"修复"。
- **每篇乐观版本**：确认按 `updated_generation` 核对目标文章是否被批改过；
  **无关文章的批改不影响确认**（它只推进全库代次，不动目标文章的版本）。
- **确认即重算**：确认事务内重新读取当前原文、重新计算候选，逐一比对
  提交的替换文本；区间越界、候选不符、版本不符、修复后超出 5–2000 字符
  限制，任一发生则**整批回滚（409）**，已选中的其他文章也不生效。
- **其余部分保持不变**：只替换选定区间，区间外文本逐字不动。
- **代次只推进一次**：整批修复（1–5 篇）合一次 `current_generation+1`
  并入队核查作业；核查发布前 `/contamination` 一律 202，不会返回
  修复前的旧污染结论。

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DB_PATH` | `/data/corpus.db` | SQLite 文件路径（多进程共库时指向同一文件） |
| `INSTANCE_ID` | 主机名 | 实例标识（作业认领署名） |
| `WORKER_INTERVAL_MS` | `1000` | 后台 worker 轮询间隔 |
| `COMPARE_DELAY_MS` | `0`（compose 中 3000） | 人为拉长比对耗时，便于观察 202 窗口与"修改中暂停比对" |
| `EXIT_WHEN_FILE_EXISTS` | 空 | 混沌/演示开关：该文件出现时进程立即硬退出（模拟崩溃重启） |

## 验证

```bash
python3 tests/e2e_check.py     # 69 项需求检查：校验、原子性、300 上限、202 门控、
                               # 桥接拆组、并发提交、旧算丢弃、崩溃重启恢复、不混版、
                               # 编码修复预览/确认、乐观版本、整批回滚、修复后重新核查
```

## 结构

```
app/
  main.py        FastAPI 端点与生命周期（含 /encoding-fix/preview|confirm）
  db.py          SQLite 连接/建表（WAL）
  similarity.py  5 字符 shingle、Jaccard、连通分量
  mojibake.py    UTF-8 误解码片段的逆转换（Latin-1 / Windows-1252）
  worker.py      后台比对 worker（租约认领、快照、原子发布）
  schemas.py     请求模型（5–2000 字符、修复区间等校验）
scripts/
  demo.py        演示数据（含桥接文章）+ 五个场景
  local_dev.sh   无 Docker 的双进程本地运行器
tests/
  e2e_check.py   端到端需求验证（自起两个进程共库）
docker-compose.yml / Dockerfile / requirements.txt
```
