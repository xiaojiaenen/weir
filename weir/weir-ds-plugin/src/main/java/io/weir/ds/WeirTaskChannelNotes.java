package io.weir.ds;

/**
 * Reference TaskChannel adapter for DolphinScheduler.
 *
 * <pre>{@code
 * // dolphinscheduler-task-weir / WeirTaskChannel.java  (against your DS version)
 * public class WeirTaskChannel implements TaskChannel {
 *   @Override
 *   public void cancelApplication(boolean status) {
 *     // cancel running Weir process if launched via TaskExecutionContext
 *   }
 *
 *   @Override
 *   public Task createTask(TaskExecutionContext ctx) {
 *     return new WeirTask(ctx);
 *   }
 * }
 * }</pre>
 *
 * <p>{@code WeirTask.handle()} should call:
 *
 * <pre>{@code
 *   WeirDsTask.Params params = WeirTaskContract.toParams(ctx.getTaskParamsMap());
 *   int code = new WeirDsTask().execute(params);
 *   if (code != 0) throw new TaskException("weir failed");
 * }</pre>
 *
 * <p>See {@code docs/ds-taskchannel.md} for factory SPI, META-INF/services registration, and
 * deployment steps.
 */
public final class WeirTaskChannelNotes {
  private WeirTaskChannelNotes() {}
}
