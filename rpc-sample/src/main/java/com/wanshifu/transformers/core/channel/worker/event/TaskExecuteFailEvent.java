package com.wanshifu.transformers.core.channel.worker.event;

import com.wanshifu.transformers.common.Event;
import com.wanshifu.transformers.common.bean.ChannelTask;

import java.util.List;

public class TaskExecuteFailEvent extends Event<List<ChannelTask>> {

    private final Exception exception;

    public TaskExecuteFailEvent(Object target, List<ChannelTask> data,Exception exception) {
        super(target, data);
        this.exception = exception;
    }

    public Exception getException() {
        return exception;
    }

}
