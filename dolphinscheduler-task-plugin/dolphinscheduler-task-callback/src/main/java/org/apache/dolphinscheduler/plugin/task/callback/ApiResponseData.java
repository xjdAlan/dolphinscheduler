/*
 * Copyright @2025 中科同昌数字能源(山西)科技有限公司版权所有
 * 项目名称： dolphinscheduler
 * 创建人：Alan
 */
package org.apache.dolphinscheduler.plugin.task.callback;

import lombok.Data;

/**
 * @Author Alan
 * @Date 2025/6/26 15:50
 */
@Data
public class ApiResponseData {
    private int code;
    private String status;
    private String data;
}