# 空气站站月文件服务

空气站数据员收到**更正日档**后，用本服务重做供数仓取数的**站月文件**。

- **FastAPI** 提供单实例网页服务（上传、状态、重试、下载）
- **DuckDB** 把单站单 UTC 日 CSV 转写为日 Parquet，并把同站同月的日档合并为月 Parquet
- **SQLite（WAL）** 保存日档/月档依赖关系与作业进度
- 全部 CSV、Parquet、数据库都写入挂载的数据卷 `/data`，容器重启不丢状态

## 数据契约

上传文件为**单站、单 UTC 日**的 CSV，表头固定：

```
station,utc_seconds,pm25
ST001,1794268800,12
```

- `station`：站号（字母数字及 `._-`，1–32 字符）
- `utc_seconds`：非负整数 UTC 秒，所有行必须落在同一个 UTC 自然日
- `pm25`：非负整数，单位 μg/m³
- 同一时间戳不得重复；一份文件只能有一个站号
- 同站同日重复上传 = **整份替换**（旧版本保留但不再是 current）

## 快速启动

```bash
docker compose up --build
# 打开 http://localhost:8000
```

数据全部位于命名卷 `station-data`（挂载到容器内 `/data`）：

```
/data
├── state.db            # SQLite：依赖与进度（WAL）
├── inbox/              # 校验通过的原始 CSV（按版本留存）
├── daily/<站号>/daily-<日>-v<版本>.parquet
├── monthly/<站号>/monthly-<月>-gen<代次>.parquet
└── tmp/                # 暂存区，任何下载入口都不会指向这里
```

本地无 Docker 时的运行方式：

```bash
python3 -m pip install -r requirements.txt
DATA_VOLUME=./data python3 -m uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 处理流水线

```
上传 CSV
  └─ 校验（失败：不写任何内容，原日档不动）
  └─ 原始 CSV 原子落盘 inbox/，登记 daily_versions(building) + 作业
        └─ 工作线程：DuckDB 写日 Parquet 到 tmp/ → fsync → 原子改名
              └─ 事务内：置 current、旧日档置 superseded、旧月档置 superseded、入队月作业
                    └─ 月作业：读取该站当月【全部 current 日档】合并
                          └─ 发布事务内再次核对签名，过期作业作废，绝不发布
```

### 关键正确性保证

| 需求 | 实现方式 |
| --- | --- |
| 保存失败保留原日档 | 校验先于任何写盘；新版本在独立事务里才切换 `is_current`，旧文件从不原地覆盖 |
| 先日后月、只重做变更日及受影响月 | 日作业优先调度；月合并只取当月 current 日档，未变更的日 Parquet 直接复用，其他月份不产生新作业 |
| 补算期间可继续更正 | 每次更正都入队新作业；同一站月仅最新月作业可发布，旧作业在出队时标记 `obsolete` |
| 过期作业不能发布，须接着补算 | 月作业在**发布事务内**重新计算日档签名，与合并前不一致即作废并删除产物，由后续作业接算 |
| 月文件对应发布时全部当前日档 | 签名 = 当月每个 current 日档的 `版本号+行数`；发布行落库依赖明细 `monthly_dependencies` |
| 日档一变旧月档标过期但仍可下载 | 旧 generation 置 `superseded`，文件不删除；`/download/generation/{id}` 仍可取 |
| 写盘/登记失败或中途退出，上次完整月文件不丢 | 新文件一律 tmp + fsync + `os.replace`；旧月档只在发布新文件的同一事务里才改状态 |
| 重启后可下载并续算 | 启动恢复：running 作业重置为 pending、清理 tmp 与孤儿文件、校验登记路径；队列全部持久化在 SQLite |
| 下载入口不指向半份/不存在文件 | 下载路径只来自登记行，响应前再查 `is_file()` 并限定在数据卷内，不满足就 303 回页面报错 |
| 页面显示是否最新、失败原因、可重试 | 仪表盘展示徽章（最新/补算中/失败/已过期）、错误文本，失败作业有“重试”按钮；页面 10 秒自动刷新，另提供 `/api/status` JSON |

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | 仪表盘页面 |
| GET | `/health` | 健康检查 |
| GET | `/api/status` | 全部日档/月档/作业状态 JSON |
| POST | `/upload` | multipart 表单字段 `file`，上传 CSV |
| POST | `/jobs/{id}/retry` | 重试失败作业（已有更新补算时会被拒绝并提示） |
| GET | `/download/monthly/{station}/{month}` | 下载当前最新月 Parquet |
| GET | `/download/generation/{gen_id}` | 下载指定历史月文件（含已过期版本） |
| GET | `/download/daily/{station}/{day}` | 下载当前日 Parquet |
| GET | `/download/daily/version/{version_id}` | 下载指定版本日 Parquet |

## 样例文件

`samples/` 附带同一站日（ST001，2026-11-10）的更正前后两份 CSV：

- `samples/before/ST001-2026-11-10.csv`：24 条小时值
- `samples/after/ST001-2026-11-10.csv`：更正 4 个错误读数并补入 2 条缺失的半小时值（26 行，整份替换）

重新生成样例：`python3 scripts/gen_samples.py`

手动验证一轮更正流程：

```bash
curl -F "file=@samples/before/ST001-2026-11-10.csv" http://localhost:8000/upload
# 等待补算完成后下载月文件，再上传更正版
curl -F "file=@samples/after/ST001-2026-11-10.csv"  http://localhost:8000/upload
```

## 测试

```bash
python3 -m pip install pytest httpx
python3 -m pytest tests/ -q
```

测试在隔离临时数据卷上启动**真实 uvicorn 子进程**，覆盖：上传/更正/复用、非法 CSV 拒绝、
过期作业作废、合并期间更正的签名重检、写盘失败保留旧档、SIGKILL 崩溃恢复与续算、
下载入口防护、历史版本仍可下载等场景（共 23 项）。

## 部署约束

必须**单实例**运行：工作线程与 SQLite WAL 都假定只有一个写者。
水平扩展需要把队列换成外部 Broker，不在本版本范围内。
