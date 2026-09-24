# 外置 Runner：把 Weir 跑在 Flink / Spark / 任意并行环境

Weir 的核心（抽取、写入、状态）是一个**无引擎依赖的普通 Java 库**（`weir-core`）。
外置引擎不需要 adapter jar，只要做两件事：

1. **拿到分片计划** —— `weir plan` 输出 JSON 契约；
2. **每个并行任务执行一个分片** —— 调 `ShardTask.run(...)`（进程内）或 `weir exec-shard`（子进程）。

分片进度落在与 CLI 相同的 StateStore（file / jdbc），所以引擎跑一半崩了，`weir full` 会接着跑；
反过来，CLI 跑了一半，引擎也能接手。写入端是幂等 merge，任务重复执行永远安全。

## 1. 分片契约

```bash
weir plan -c job.yaml
```

```json
{
  "job": "orders-sync",
  "planId": "efb00afa-133",
  "resolvedSplit": "RANGE",
  "splitColumn": "id",
  "shards": [
    {"index": 0, "bounds": "[1,3)", "predicate": "id >= 1 AND id < 3", "fullScan": false},
    {"index": 1, "bounds": "[3,5)", "predicate": "id >= 3 AND id < 5", "fullScan": false},
    {"index": 2, "bounds": "[5,∞)", "predicate": "id >= 5", "fullScan": false}
  ],
  "command": "weir exec-shard -c <job.yaml> --shard-index <index> --plan-id efb00afa-133"
}
```

`planId` 是分片计划的指纹（表 + 投影 + 每个分片的谓词）。源表增长导致分片边界变化时指纹变化，
旧的进度会被丢弃而不是被信任；引擎侧传了过期的 `--plan-id` 也会被直接拒绝（exit 1），
避免用旧边界漏读新数据。

## 2. 进程内执行（推荐给 Flink / Spark）

```java
// 依赖：io.weir:weir-core（以及目标 writer 模块），无需任何 flink/spark sdk 编译期耦合
ShardTask.Result r = ShardTask.run(config, shardIndex, planId);
// r.rows() / r.maxWatermark() / r.skipped()（该分片之前已完成时为 true）
```

`ShardTask.run` 单次调用完成：打开 writer → 抽取该分片 → 攒批写入 → flush → 记分片完成。
调用方无需管状态。

## 3. Flink 1.20 驱动示例

```java
// build.gradle: implementation "io.weir:weir-core:0.1.0-SNAPSHOT"
// flink 依赖由你的 Flink 集群提供（provided）
public static void main(String[] args) throws Exception {
    JobConfig config = ConfigLoader.load(Path.of(args[0]));
    List<Integer> shards;
    try (JdbcExtractor ex = new JdbcExtractor(config)) {
        SplitPlanner.Plan plan = ex.planShards();
        shards = java.util.stream.IntStream.range(0, plan.shards().size()).boxed().toList();
    }

    StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
    env.setRuntimeMode(RuntimeExecutionMode.BATCH);
    env.setParallelism(Math.min(shards.size(), config.runtime.extractThreads));

    env.fromCollection(shards)
        .map((MapFunction<Integer, String>) idx -> {
            // 每个 subtask 独立开连接与 writer；幂等 merge 保证重试安全
            return ShardTask.run(config, idx, null).toLine();
        })
        .print();

    env.execute("weir-" + config.name);
}
```

> 想做 Flink 原生 FLIP-27 Source / Exactly-once sink 属于后续迭代；
> 当前形态下「分片 = 并行单元 + 状态在 Weir 侧」已经能吃满 task slots。

## 4. Spark 3.5 驱动示例

```java
// JavaSparkContext sc = new JavaSparkContext(conf);
List<Integer> shards = /* 同上，planShards() */;
JavaRDD<Integer> rdd = sc.parallelize(shards, Math.min(shards.size(), config.runtime.extractThreads));
rdd.foreachPartition(indexes -> {
    // 一个 partition 复用一个连接配置；每片独立 checkpoint
    for (Integer idx : indexes) {
        ShardTask.run(config, idx, null);
    }
});
```

## 5. 纯 Shell 并行（不写 Java 也行）

```bash
PLAN=$(weir plan -c job.yaml)
for i in 0 1 2 3; do
  ssh worker$i "/opt/weir/bin/weir exec-shard -c job.yaml --shard-index $i --plan-id $(echo $PLAN | jq -r .planId)" &
done
wait
```

注意：`exec-shard` 与 `weir full` 都从**同一个 StateStore** 读写进度；
如果 worker 不共享文件系统，请用 `state.type: jdbc`（所有节点连同一个状态库）。

## 6. 一致性语义（与 CLI 相同）

| 保证 | 说明 |
|------|------|
| 分片级 checkpoint | 每个分片写成功后才记录完成；崩溃后从第一个未完成分片续跑 |
| planId 失效保护 | 表结构 / 分片配置变化 → 旧进度作废，宁可重读不漏读 |
| effectively-once | 目标端 PK merge 吸收任何重复（引擎重试、混跑 CLI 都安全） |
| 水位一次提交 | 全部分片完成、进度清零后才推进水位 |
