package io.weir.ds;

import org.apache.dolphinscheduler.plugin.task.api.TaskChannel;
import org.apache.dolphinscheduler.plugin.task.api.TaskChannelFactory;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;

/** Factory + channel for DS 3.4.x. */
public class WeirTaskChannelFactory implements TaskChannelFactory {
  @Override
  public String getName() {
    return WeirTaskContract.TASK_TYPE;
  }

  @Override
  public TaskChannel create() {
    return new WeirTaskChannel();
  }

  public static class WeirTaskChannel implements TaskChannel {
    @Override
    public WeirTask createTask(TaskExecutionContext ctx) {
      return new WeirTask(ctx);
    }

    @Override
    public org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters parseParameters(
        String taskParams) {
      return WeirParameters.parse(taskParams);
    }
  }
}
