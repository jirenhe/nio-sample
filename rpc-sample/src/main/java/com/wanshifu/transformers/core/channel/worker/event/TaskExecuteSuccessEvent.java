package com.wanshifu.transformers.core.channel.worker.event;

import com.wanshifu.transformers.common.Event;
import com.wanshifu.transformers.common.bean.ChannelTask;

import java.util.List;

public class TaskExecuteSuccessEvent extends Event<List<ChannelTask>> {

    public TaskExecuteSuccessEvent(Object target, List<ChannelTask> data) {
        super(target, data);
    }
}
