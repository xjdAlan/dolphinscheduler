package org.apache.dolphinscheduler.plugin.task.callback;

import lombok.Data;
import org.apache.commons.lang3.StringUtils;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;


@Data
public class CallbackParameters extends AbstractParameters {

    /**
     * 任务id
     */
    private Long taskId;

    /**
     * 任务类型：实时/批量
     */
    private TaskRealType taskRealType;

    /**
     * 启动任务 url
     */
    private String startUrl;

    /**
     * 获取任务执行状态 url
     */
    private String statusUrl;

    /**
     * 获取任务状态 间隔时间 秒
     */
    private Integer pollInterval = 5;

    /**
     * 获取任务状态最大轮询次数，超过次数直接失败（只针对批量任务）
     */
    private Integer maxPollAttempts = 10;

    /**
     * Check that the parameters are valid
     *
     * @returnboolean
     */
    @Override
    public boolean checkParameters() {
        return StringUtils.isNotBlank(startUrl) && StringUtils.isNotBlank(statusUrl);
    }
}
