
package org.apache.dolphinscheduler.plugin.task.callback;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.TaskChannel;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.parameters.ParametersNode;
import org.apache.dolphinscheduler.plugin.task.api.parameters.resource.ResourceParametersHelper;

public class CallbackTaskChannel implements TaskChannel {

    @Override
    public void cancelApplication(boolean status) {

    }

    @Override
    public CallbackTask createTask(TaskExecutionContext taskRequest) {
        return new CallbackTask(taskRequest);
    }

    @Override
    public AbstractParameters parseParameters(ParametersNode parametersNode) {
        return JSONUtils.parseObject(parametersNode.getTaskParams(), CallbackParameters.class);
    }

    @Override
    public ResourceParametersHelper getResources(String parameters) {
        return null;
    }
}
