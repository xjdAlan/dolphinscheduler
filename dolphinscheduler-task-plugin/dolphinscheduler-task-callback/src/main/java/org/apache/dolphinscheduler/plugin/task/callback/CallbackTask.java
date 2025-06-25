
package org.apache.dolphinscheduler.plugin.task.callback;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.dolphinscheduler.common.utils.JSONUtils;
import org.apache.dolphinscheduler.plugin.task.api.AbstractTask;
import org.apache.dolphinscheduler.plugin.task.api.TaskCallBack;
import org.apache.dolphinscheduler.plugin.task.api.TaskException;
import org.apache.dolphinscheduler.plugin.task.api.TaskExecutionContext;
import org.apache.dolphinscheduler.plugin.task.api.model.Property;
import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;
import org.apache.dolphinscheduler.plugin.task.api.utils.ParameterUtils;
import org.apache.http.HttpEntity;
import org.apache.http.ParseException;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.*;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class CallbackTask extends AbstractTask {

    /**
     * callback parameters
     */
    private CallbackParameters callbackParameters;
    /**
     * taskExecutionContext
     */
    private TaskExecutionContext taskExecutionContext;


    private String dataCenterPrefixUrl;
    private String dsAgentPrefixUrl;

    /**
     * constructor
     *
     * @param taskExecutionContext taskExecutionContext
     */
    protected CallbackTask(TaskExecutionContext taskExecutionContext) {
        super(taskExecutionContext);
        this.taskExecutionContext = taskExecutionContext;
    }

    @Override
    public void init() {
        this.callbackParameters = JSONUtils.parseObject(taskExecutionContext.getTaskParams(), CallbackParameters.class);
        log.info("Initialize callback task params {}", JSONUtils.toPrettyJsonString(callbackParameters));

        if (callbackParameters == null || !callbackParameters.checkParameters()) {
            throw new RuntimeException("callback task params is not valid");
        }
        //TODO
        dataCenterPrefixUrl = System.getenv("DATA_CENTER_PREFIX_URL");
        dsAgentPrefixUrl = System.getenv("DS_AGENT_PREFIX_URL");
    }

    @Override
    public void handle(TaskCallBack taskCallBack) throws TaskException {
        // 调用startUrl开启任务，如果调用失败，任务节点状态直接失败，结束
        /* 调用成功，则开始轮询任务状态，定时调用statusUrl获取任务节点执行状态
              如果是批处理任务：
                如果状态返回成功，直接修改任务节点状态为成功，结束
                如果状态返回运行中，不做操作，继续调度
                如果状态返回失败，任务节点状态直接失败，结束
                如果statusUrl调用失败，继续调度，直到到达最大轮询次数（默认50次），任务节点状态直接失败，结束（此场景需要考虑如果设置了重试，ds会重新执行任务，需startUrl接口考虑如何处理）
              如果是实时任务：
                如果状态返回运行中，不做操作，继续调度
                如果状态返回失败，重新调用startUrl重启任务，然后继续重复以上流程（需要保证startUrl和statusUrl中的任务id不变）
                如果状态返回取消（代表手动取消任务），任务节点状态成功，结束
                如果statusUrl调用失败，继续调度，直到到达最大轮询次数（默认50次），任务节点状态直接失败，结束（特殊考虑同批处理）
         */

        //开始任务
        if (!startTask()) {
            return;
        }

        //开始轮询任务状态
        int failCount = 0;
        while(true) {
            try {
                Thread.sleep(callbackParameters.getPollInterval() * 1000);
            } catch (InterruptedException e) {
                log.error("Thread.sleep error, taskId:{}, taskInstanceId:{}",
                        taskExecutionContext.getTaskCode(), taskExecutionContext.getTaskInstanceId(), e);
            }

            try (CloseableHttpClient client = createHttpClient();
                 CloseableHttpResponse response = sendRequest(client, dataCenterPrefixUrl + callbackParameters.getStatusUrl())) {
                int statusCode = response.getStatusLine().getStatusCode();
                String body = getResponseBody(response);
                Map<String, String> responseMap = JSONUtils.toMap(body);
                if (!"200".equals(statusCode) || responseMap == null || !"0".equals(responseMap.get(respCode))) {
                    failCount++;
                    log.error("{}请求获取状态失败，失败第{}次，taskId：{}，taskInstanceId：{}",
                            callbackParameters.getStatusUrl(), failCount,
                            taskExecutionContext.getTaskCode(), taskExecutionContext.getTaskInstanceId());
                    if (failCount >= callbackParameters.getMaxPollAttempts()) {
                        log.error("{}请求获取状态失败次数超过最大限制，任务失败，taskId：{}，taskInstanceId：{}",
                                callbackParameters.getStatusUrl(), taskExecutionContext.getTaskCode(), taskExecutionContext.getTaskInstanceId());
                        exitStatusCode = -1;
                        return;
                    }
                    continue;
                }

                String status = responseMap.get(respStatus);
                switch (callbackParameters.getTaskRealType()) {
                    case BATCH -> {
                        if (TaskStatus.RUNNING.toString().equals(status)) {

                        } else if (TaskStatus.SUCCESS.toString().equals(status)) {
                            return;
                        }
                        else if (TaskStatus.FAILD.toString().equals(status)) {
                            exitStatusCode = -1;
                            return;
                        }
                    } case REALTIME -> {
                        if (TaskStatus.RUNNING.toString().equals(status)) {

                        } else if (TaskStatus.FAILD.toString().equals(status)) {
                            exitStatusCode = -1;
                            // 重新启动新的任务实例
                            for (int i = 0; i < 3; i++) {
                                if (restartTask()) {
                                    break;
                                }
                            }
                        } else if (TaskStatus.CANCEL.toString().equals(status)) {
                            return;
                        }

                    }
                }
            } catch (Exception e) {
                log.error("{}请求获取状态异常，taskId：{}，taskInstanceId：{}",
                        callbackParameters.getStatusUrl(),
                        taskExecutionContext.getTaskCode(), taskExecutionContext.getTaskInstanceId(), e);
                exitStatusCode = -1;
                throw new TaskException("Execute callback task failed", e);
            }
        }
    }


    private boolean startTask() {
        boolean result = true;
        try (CloseableHttpClient client = createHttpClient();
             CloseableHttpResponse response = sendRequest(client, dataCenterPrefixUrl + callbackParameters.getStartUrl())) {
            int statusCode = response.getStatusLine().getStatusCode();
            String body = getResponseBody(response);
            Map<String, String> responseMap = JSONUtils.toMap(body);
            if (!"200".equals(statusCode) || responseMap == null || !"0".equals(responseMap.get(respCode))) {
                log.error("{}任务开始失败，http响应码：{}，response响应码：{}，任务id：{}，任务实例id：{}",
                        callbackParameters.getStartUrl(), statusCode,
                        responseMap == null ? null : responseMap.get(respCode), taskExecutionContext.getTaskCode(), taskExecutionContext.getTaskInstanceId());
                exitStatusCode = -1;
                result = false;
            }
        } catch (Exception e) {
            exitStatusCode = -1;
            log.error("httpUrl[" + callbackParameters.getStartUrl() + "] connection failed" , e);
            throw new TaskException("Execute callback task failed", e);
        }
        return result;
    }

    private boolean restartTask() {
        boolean result = true;
        try (CloseableHttpClient client = createHttpClient();
             CloseableHttpResponse response = sendRequestPost(client, dsAgentPrefixUrl)) {
            int statusCode = response.getStatusLine().getStatusCode();
            String body = getResponseBody(response);
            Map<String, String> responseMap = JSONUtils.toMap(body);
            if (!"200".equals(statusCode) || responseMap == null || !"0".equals(responseMap.get(respCode))) {
                log.error("{}任务重启失败，http响应码：{}，response响应码：{}，任务code：{}",
                        dsAgentPrefixUrl, statusCode,
                        responseMap == null ? null : responseMap.get(respCode), taskExecutionContext.getProcessDefineCode());
                result = false;
            }
        } catch (Exception e) {
            log.error("httpUrl[" + dsAgentPrefixUrl + "] connection failed" , e);
            result = false;
        }
        return result;
    }

    @Override
    public void cancel() throws TaskException {

    }

    @Override
    public AbstractParameters getParameters() {
        return this.callbackParameters;
    }



    private CloseableHttpClient createHttpClient() {
        final RequestConfig requestConfig = requestConfig();
        HttpClientBuilder httpClientBuilder;
        httpClientBuilder = HttpClients.custom().setDefaultRequestConfig(requestConfig);
        return httpClientBuilder.build();
    }

    /**
     * request config
     *
     * @return RequestConfig
     */
    private RequestConfig requestConfig() {
        return RequestConfig.custom().setSocketTimeout(socketTimeout)
                .setConnectTimeout(connectTimeout).build();
    }


    private CloseableHttpResponse sendRequest(CloseableHttpClient client, String url) throws
            IOException, URISyntaxException {

        URI uri = new URIBuilder(url)
                .setParameter(paramTaskId, String.valueOf(taskExecutionContext.getTaskCode()))
                .setParameter(paramTaskInstanceId, String.valueOf(taskExecutionContext.getTaskInstanceId()))
                .build();

        HttpGet httpGet = new HttpGet(uri);
        httpGet.setHeader("Accept", "application/json");

        return client.execute(httpGet);
    }

    private CloseableHttpResponse sendRequestPost(CloseableHttpClient client, String url) throws
            IOException {

        // 构造请求参数（JSON）
        Map<String, Object> params = new HashMap<>();
        params.put(agentParamTaskCode, taskExecutionContext.getProcessDefineCode());

        HttpPost httpPost = new HttpPost(url);
        httpPost.setHeader("Content-Type", "application/json");
        httpPost.setHeader("Accept", "application/json");
        httpPost.setEntity(new StringEntity(JSONUtils.toJsonString(params), "UTF-8"));

        return client.execute(httpPost);
    }

    private String getResponseBody(CloseableHttpResponse httpResponse) throws ParseException, IOException {
        if (httpResponse == null) {
            return null;
        }
        HttpEntity entity = httpResponse.getEntity();
        if (entity == null) {
            return null;
        }
        return EntityUtils.toString(entity, StandardCharsets.UTF_8.name());
    }



    private final int socketTimeout = 5000;
    private final int connectTimeout = 3000;
    private final String paramTaskInstanceId = "taskInstanceId";
    private final String paramTaskId = "taskId";
    private final String respStatus = "status";
    private final String respCode = "code";

    private final String agentParamTaskCode = "code";
}
