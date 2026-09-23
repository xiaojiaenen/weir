# Weir

无 CDC 权限场景下的 JDBC **全量 / 增量** 同步组件。支持自增 `id`、`update_time(+id)` 增量，目标端以 **幂等 merge** 保证 effectively-once（任务可安全重跑）。调度对接 **DolphinScheduler**。

设计文档见 [`../jdbc-sync-design.md`](../jdbc-sync-design.md)。

## 版本基线

| 组件 | 版本 |
|------|------|
| JDK | 17+ |
| Flink（后续 Runner） | 1.20.x |
| Spark（后续 Runner） | 3.5.x |
| Paimon（后续 Writer） | 1.0.1 |
| DolphinScheduler | 3.2+（`weir` 任务） |

## 模块

```
weir/
  weir-api            配置、模型、SPI、方言（Dialect 同时服务源端与 sink）
  weir-core           抽取、分片规划、水位、diff、preflight、指标
  weir-writer-file    JSONL/CSV 落盘
  weir-writer-jdbc    多方言 upsert（经 Dialect 生成，无重复实现）
  weir-writer-console 调试输出
  weir-writer-kafka   JSON，key=PK
  weir-writer-doris   FE Stream Load
  weir-writer-paimon  FileStoreTable Java API
  weir-cli            命令行入口
  weir-dist           Hadoop 风格发行版（bin/ conf/ lib/）
  weir-ds-plugin      DolphinScheduler 任务适配
  examples/           任务 YAML 样例
```

## 安装（类似 Hadoop）

不必只丢 fat-jar，可装成发行版目录：

```bash
cd weir
mvn -q -DskipTests package
# 产物：weir-dist/target/weir-0.1.0-SNAPSHOT.tar.gz  （顶层即 bin/ conf/ lib/）

mkdir -p /opt/weir
tar -xzf weir-dist/target/weir-0.1.0-SNAPSHOT.tar.gz -C /opt/weir
export PATH=/opt/weir/bin:$PATH
export JAVA_HOME=/path/to/jdk17

weir check -c /opt/weir/conf/job-console.yaml
weir state -c /opt/weir/conf/job-console.yaml
```

目录布局：

```text
weir-0.1.0-SNAPSHOT/
  bin/weir          # 启动脚本（同 hadoop/bin/hadoop）
  conf/weir-env.sh  # JAVA_HOME / WEIR_OPTS
  conf/*.yaml       # 任务样例
  lib/              # 依赖 jar
  ext/              # 自备驱动（oracle/mssql 等）
  examples/         # 任务 YAML 样例
  demo/             # H2 演示源库脚本 init.sql（首次连接自动建库）
```

> 示例 YAML 里的 `./demo/...`、`./out/...` 都是**相对当前工作目录**解析的，在哪个目录执行 `weir` 就从哪里起算。

装好后可直接试内置 demo：

```bash
cd /opt/weir
weir check -c examples/h2-demo.yaml
weir full  -c examples/h2-demo.yaml
cat demo/out/orders.jsonl
```

## 源 / 写入扩展

| 源方言 | 增量 | 分片（range/mod/hash） | sink upsert | 加列 DDL |
|--------|------|------------------------|-------------|----------|
| MySQL / MariaDB | ✓ | ✓ / ✓ / CRC32 | `ON DUPLICATE KEY` | ✓ |
| H2 | ✓ | ✓ / ✓ / — | `MERGE ... KEY` | ✓ |
| PostgreSQL | ✓ | ✓ / ✓ / hashtext | `ON CONFLICT` | ✓ |
| Oracle | ✓ | ✓ / ✓ / ORA_HASH | `MERGE INTO` | ✓（括号语法） |
| SQL Server | ✓ | ✓ / ✓ / CHECKSUM | `MERGE` | ✓ |

| Writer | 说明 | 可被 diff 读回 |
|--------|------|----------------|
| `jdbc` | 多方言 merge + 软删 DELETE | ✓ |
| `file` | jsonl / csv / tsv | ✗（仅支持 `soft_column`） |
| `console` | 调试打印 | ✗ |
| `kafka` | JSON，key=PK，幂等生产 | ✗ |
| `doris` | FE Stream Load，支持 `__DORIS_DELETE_SIGN__` | ✗ |
| `paimon` | FileStoreTable Java API，PK upsert / DELETE RowKind | ✗ |

> `deleteDetect.mode` 为 `pk_diff` / `fingerprint` 时要求目标可被扫描回读；写-only 目标（file/kafka/doris/paimon）会 **直接报错** 而不是把「读不到」当成「目标为空」全量重灌。这类目标请用 `soft_column`。

## 能力矩阵

| 能力 | 状态 | 说明 |
|------|------|------|
| 全量抽取（半开分片 + 并行） | ✅ | `splits.strategy: auto\|range\|mod\|hash\|none` |
| 非数值主键分片 | ✅ | 数值走 range/mod，字符串键走 hash，不支持则降级单分片并告警 |
| 增量 `id` / `update_time` / `update_time_id` + overlap | ✅ | 同时间戳按 id 兜底；overlap 找回回填行 |
| 单趟行数上限 | ✅ | `incremental.maxRowsPerRun` |
| query 模式（join SQL + 契约列） | ✅ | 仅支持 `none`/`soft_column` 删除感知 |
| 水位 StateStore，写成功后才提交 | ✅ | file / jdbc |
| 源库保护 | ✅ | 连接池 `poolMax`、并发信号量、令牌桶 rows/sec、带抖动退避重试 |
| Preflight 前置检查 | ✅ | 增量列存在性、缺失索引、水位回退、目标可达、overlap=0 |
| 指标与运行报告 | ✅ | `RunMetrics` + `RunReport`，JSON 落盘 |
| 抽样哈希核对 | ✅ | `quality.sampleHashCheck` |
| 软删 → DELETE / Doris / Paimon | ✅ | 支持自定义真值 `softTrueValues` |
| PK Diff（insert/delete/rewrite） | ✅ | 值规范化比对，二次 diff 为零写入 |
| 分区指纹 diff | ✅ | 指纹未变则跳过行级扫描 |
| Schema 加列自动演进 | ✅ | 各方言 DDL |
| CLI 完整运维命令 | ✅ | run/full/incremental/diff/check/state/runs/reset |
| DS TaskChannel 插件 | ✅ | 见 `docs/ds-taskchannel.md` |
| Flink / Spark 外置 Runner | 🔜 | 核心逻辑已与引擎解耦 |

## 5 分钟体验（内置 H2 demo）

不需要 MySQL，也不需要先建库：`examples/h2-demo.yaml` 的 JDBC URL 带了 `INIT=RUNSCRIPT FROM './demo/init.sql'`，**第一次连接时 H2 会自动建表并灌入 5 行订单数据**。示例里的 `./demo/...` 是相对路径，请在 `weir/` 目录下执行：

```bash
cd weir
mvn -q -DskipTests package

JAR=weir-cli/target/weir-cli-0.1.0-SNAPSHOT.jar

java -jar $JAR check -c examples/h2-demo.yaml            # 前置检查：列存在性 / 索引 / 水位 / 目标可达
java -jar $JAR full  -c examples/h2-demo.yaml            # 全量：读 5 行，写 5 行
cat demo/out/orders.jsonl                                # 目标端产物

java -jar $JAR state -c examples/h2-demo.yaml            # 水位：id=5
java -jar $JAR incremental -c examples/h2-demo.yaml      # 再跑一次：0 行（幂等、可重跑）

java -jar $JAR runs  -c examples/h2-demo.yaml -n 10      # 运行历史
java -jar $JAR reset -c examples/h2-demo.yaml            # 清水位，下次重新引导
```

想自己造数据（源库改动后再跑 `incremental` 就能看到增量被抓到）：

```bash
java -cp ~/.m2/repository/com/h2database/h2/2.2.224/h2-2.2.224.jar org.h2.tools.Shell \
     -url "jdbc:h2:./demo/src;MODE=MySQL" -user sa -password "" \
     -sql "INSERT INTO orders(id,user_id,amount,update_time) VALUES(6,14,3.30,TIMESTAMP '2024-01-02 09:00:00')"
```

想彻底重来：`rm -f demo/src.mv.db demo/src.trace.db`，下次运行会自动按 `demo/init.sql` 重建（脚本幂等）。仓库不提交 `src.mv.db` 二进制文件。

## 使用

```bash
java -jar weir-cli/target/weir-cli-*.jar <command> -c <job.yaml> [-n <limit>]

weir check        -c examples/mysql-to-file.yaml   # 校验配置/连通性/索引/水位
weir full         -c examples/mysql-to-file.yaml   # 全量快照
weir incremental  -c examples/mysql-to-file.yaml   # 按 id / update_time 增量
weir diff         -c examples/mysql-to-file.yaml   # 软删 / pk-diff / 指纹修正
weir state        -c examples/mysql-to-file.yaml   # 查看水位
weir runs         -c examples/mysql-to-file.yaml -n 10   # 运行历史
weir reset        -c examples/mysql-to-file.yaml   # 清水位，下次重新引导
```

退出码：`0` 成功，`1` 运行失败或质量门禁拦截，`2` 用法错误。

## 一致性语义

| 保证 | 说明 |
|------|------|
| 管道 at-least-once | 失败重试可能重读 |
| **业务 effectively-once** | 目标端 PK merge + 写成功后才推水位 |
| 并行分片 | `id` 半开区间 `[lo,hi)` 不重叠 |
| `update_time` | 允许 overlap 重读，由 merge/去重收敛 |

**不承诺**无 CDC 下的管道字节级 exactly-once 或删除事件恰好一次（删除用软删列 / diff 最终一致）。

## 关键设计点

### 值是「变了」还是「只是类型不同」

行比对统一走 `RowValues.normalize`：`Instant` / `Timestamp` / ISO 字符串归到 epoch 毫秒，`1`、`1L`、`1.0`、`DECIMAL(1.00)` 归到同一个规范串。否则源端 `Instant` 与目标端 `Timestamp` 会被判成全表变化，每次 diff 重写整表。

### 分片规划

1. 先探 `MIN/MAX` 判列是否数值；
2. 数值 → `range`（连续）或 `mod`（稀疏 id）；
3. 非数值 → 厂商哈希分片（`CRC32` / `hashtext` / `ORA_HASH` / `CHECKSUM`）；
4. 都不支持 → 降级单分片并告警，绝不产出坏 SQL。

### Sink 的 DML 按列集合缓存

同一个 diff 批次里既有「只有主键 + `_op=d`」的删除标记，也有完整行。Sink 按列集合分组建语句，避免删除标记把后续整行的 upsert 语句降级成只写主键。

## 配置要点

- `source.read.mode`: `table`（推荐）或 `query`（必须投影 `primaryKey` + 增量列）
- `source.read.incremental.strategy`: `id` | `update_time` | `update_time_id`
- `source.read.splits.strategy`: `auto` | `range` | `mod` | `hash` | `none`
- `target.type`: `file` | `jdbc` | `console` | `kafka` | `doris` | `paimon`
- `target.writeMode: merge` 时必须配置 `primaryKey`
- `state`: `file`（默认）或 `jdbc`
- `runtime.reportPath` 非空时每次运行落一份 JSON 报告

完整样例见 `examples/`。

## DolphinScheduler TaskChannel

完整步骤见 **[docs/ds-taskchannel.md](docs/ds-taskchannel.md)**。摘要：

1. 按集群 DS 版本加 `dolphinscheduler-task-api`（推荐 3.4.3，provided）
2. 实现 `WeirTaskChannelFactory`（name=`WEIR`）+ `WeirTaskChannel` + `WeirTask`
3. SPI：`META-INF/services/...TaskChannelFactory` → `io.weir.ds.WeirTaskChannelFactory`
4. 插件 jar 放到 API/Worker `libs` 后重启
5. UI 任务参数：`configFile` + `mode`（契约见 `WeirTaskContract`）

MVP 可用 Shell：`/opt/weir/bin/weir incremental -c /path/job.yaml`。

## 构建与测试

```bash
mvn -q -DskipTests package
mvn -q test        # 56 个用例（core 54 + paimon 2），含 H2 端到端与 Paimon 本地表
```
