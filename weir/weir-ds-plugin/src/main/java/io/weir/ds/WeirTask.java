package io.weir.ds;

import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

/** DS 3.4 task body — runs Weir in-process. */
public class WeirTask extends AbstractTask {
  private final WeirParameters parameters;

  public WeirTask(TaskExecutionContext taskRequest) {
    super(taskRequest);
    this.parameters = WeirParameters.parse(taskRequest.getTaskParams());
  }

  @Override
  public void handle(TaskCallBack taskCallBack) throws TaskException {
    try {
      int code = new WeirDsTask().execute(parameters.toWeirParams());
      setExitStatusCode(code);
      setAppIds("");
      if (code != 0) {
        throw new TaskException("weir exit code " + code);
      }
    } catch (TaskException e) {
      throw e;
    } catch (Exception e) {
      throw new TaskException("weir task failed: " + e.getMessage(), e);
    }
  }

  @Override
  public void cancel() throws TaskException {
    // in-process run; nothing to kill asynchronously
  }

  @Override
  public AbstractParameters getParameters() {
    return parameters;
  }
}
