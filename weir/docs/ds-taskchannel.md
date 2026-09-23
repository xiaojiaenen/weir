# DolphinScheduler TaskChannel 接入（weir）

> **DS 版本**：最新稳定 **3.4.3**（2026-09）。插件以集群实际版本为准；3.2 / 3.3 / 3.4 SPI 包名略有差异，对照同发行版的 `dolphinscheduler-task-datax`。

目标：在 DS 任务类型里出现 **`WEIR`**，参数填 YAML 路径/内容 + 同步模式。

## 架构

```text
DS Master/Worker
  └─ task plugin jar (dolphinscheduler-task-weir)
       ├─ WeirTaskChannelFactory  → 注册类型 "WEIR"
       ├─ WeirTaskChannel         → createTask / cancel
       └─ WeirTask.handle()
            └─ WeirDsTask.execute(configFile|configYaml, mode)
                 └─ WeirRunner（水位、抽取、幂等写）
```

DS **只负责触发与日志**；**水位在 Weir StateStore**（推荐元数据库 JDBC），与 Task 实例 ID 无关。

## 1. 最快路径（先跑通）

用 Shell 任务：

```bash
/opt/weir/bin/weir incremental -c /opt/weir/conf/orders.yaml
```

失败重试会从 StateStore 水位续跑，目标 merge 幂等。

## 2. 正式 TaskChannel 插件

### 2.1 依赖（按你们 DS 版本）

```xml
<dependency>
  <groupId>org.apache.dolphinscheduler</groupId>
  <artifactId>dolphinscheduler-task-api</artifactId>
  <version>3.2.2</version> <!-- 与集群一致 -->
  <scope>provided</scope>
</dependency>
```

3.1 / 3.2 / 3.3 的包名略有差异，以集群里 `dolphinscheduler-task-datax` 反编译/源码为准（对齐 `task-datax` / `task-seatunnel`）。

### 2.2 工厂

```java
package io.weir.ds;

import org.apache.dolphinscheduler.spi.task.TaskChannel;
import org.apache.dolphinscheduler.spi.task.TaskChannelFactory;
// 3.2+ 也可能是 org.apache.dolphinscheduler.plugin.task.api.TaskChannelFactory

public class WeirTaskChannelFactory implements TaskChannelFactory {
  @Override
  public TaskChannel create() {
    return new WeirTaskChannel();
  }

  @Override
  public String getName() {
    return WeirTaskContract.TASK_TYPE; // "WEIR"
  }

  // 部分版本还要 getOptions() / getParamsMap()，照抄 datax 插件即可
}
```

### 2.3 Channel + Task

```java
public class WeirTaskChannel implements TaskChannel {
  @Override
  public void cancelApplication(boolean cancel) {
    // 若用 ProcessBuilder 起 weir 进程，在此 destroy
  }

  @Override
  public Task createTask(TaskExecutionContext ctx) {
    return new WeirTask(ctx);
  }
}

public class WeirTask extends AbstractTask {
  public WeirTask(TaskExecutionContext ctx) {
    super(ctx);
  }

  @Override
  public void handle() throws Exception {
    Map<String, String> params = taskRequest.getTaskParamsMap();
    WeirDsTask.Params p = WeirTaskContract.toParams(params);
    int code = new WeirDsTask().execute(p);
    if (code != 0) {
      throw new TaskException("weir exit " + code);
    }
    setExitStatusCode(code);
    setAppIds(""); // 无 YARN 应用时可空
  }
}
```

### 2.4 SPI 注册

```text
src/main/resources/META-INF/services/org.apache.dolphinscheduler.spi.task.TaskChannelFactory
```

内容一行：

```text
io.weir.ds.WeirTaskChannelFactory
```

（类名与你们 DS SPI 接口 FQCN 一致；3.2+ 可能是  
`org.apache.dolphinscheduler.plugin.task.api.TaskChannelFactory`）

### 2.5 部署

1. `mvn -pl weir-ds-plugin -am package`
2. 把 `weir-ds-plugin` + `weir-core` + writers + `weir-cli` 打成 **一个 plugin jar**（shade），或按 DS 插件目录规范放 `lib`
3. 拷到 **每台 API Server 与 Worker** 的任务插件目录（常见）：
   - `api-server/libs/`
   - `worker-server/libs/`  
   或 `dolphinscheduler-task-plugin/dolphinscheduler-task-weir/target/*.jar`
4. 滚动重启 API / Worker
5. UI → 任务定义 → 类型 **WEIR**
   - `configFile`: `/opt/weir/conf/orders.yaml`
   - `mode`: `incremental` | `full` | `diff` | `check`
   - 或 `configYaml`: 直接贴 YAML（适合小任务/测试）

### 2.6 参数契约（已在代码中）

| 键 | 说明 |
|----|------|
| `configFile` | 任务 YAML 路径（推荐，配置进 Git） |
| `configYaml` | 内联 YAML（与 configFile 二选一） |
| `mode` | 同步模式 |

映射见 `io.weir.ds.WeirTaskContract`。

### 2.7 与补数 / 告警

- **补数**：DS 补数只是多次触发；偏移量仍以 `weir_state` 为准，不要把 offset 写进 DS 参数
- **重跑**：安全（at-least-once + merge）
- **告警**：非 0 退出码即可接 DS 失败告警
- **退出码**：`0` 成功；`1` 运行失败或质量门禁拦截（`quality.failOnQualityMismatch: true`）；`2` 用法错误

### 2.8 运行报告

每次运行都会产出 `RunReport`（状态、行数、批次数、重试数、分片数、耗时、rows/s、水位、滞后、质量结论）：

- `state.type: file` → `<state.path>/runs.jsonl`（`weir runs -c job.yaml -n 20` 可读）
- `state.type: jdbc` → `weir_run` 表
- `runtime.reportPath` 非空时额外落一份 JSON 文件，便于被外部采集

`WeirTask.handle()` 里可把 `SyncResult.report()` 的 `status` / `message` 写进任务日志，DS 失败节点能直接看到原因，不必登机器翻日志。

`quality.failOnQualityMismatch: true` 时，行数或抽样哈希核对不通过会让任务以退出码 `1` 结束——**建议稳定后再打开**，否则目标端本来就落后时会持续告警。

## 3. 版本差异速查

| DS | SPI 典型包 | 备注 |
|----|------------|------|
| **3.4.x（推荐）** | `org.apache.dolphinscheduler.plugin.task.api.*` | 对照 `task-datax` 3.4 源码 |
| 3.3.x | 同上 | |
| 3.2.x | plugin.task.api + spi 并存 | |
| 3.1.x | `org.apache.dolphinscheduler.spi.task.*` | 老 SPI |

**原则**：以集群内 `dolphinscheduler-task-datax-*.jar` 为模板，把 `DATAX` 换成 `WEIR`，`handle()` 换成 `WeirDsTask.execute`。
