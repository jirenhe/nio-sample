package com.wanshifu.transformers.common.bean;

import java.io.Serializable;

public class ChannelTask extends Task implements Serializable {

    private final String taskId;

    private int retryTimes = 0;

    public ChannelTask(Task task, String id) {
        super(task.getDataSource(), task.getDb(), task.getTable(), task.getEventType(), task.getRecord());
        this.taskId = id;
    }

    public String getTaskId() {
        return taskId;
    }

    public int getRetryTimes() {
        return retryTimes;
    }

    public void increaseRetryTimes() {
        this.retryTimes++;
    }

}
