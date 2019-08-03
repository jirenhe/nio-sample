package com.wanshifu.transformers.core.channel.collector;

import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.taskpool.TaskPool;

import java.util.List;

public class SampleCollector extends collector {

    public SampleCollector(TaskPool taskPool) {
        super(taskPool);
    }

    @Override
    protected void postInit() {

    }

    @Override
    protected List<ChannelTask> handleFailureTask(List<ChannelTask> channelTasks) {
        return channelTasks;
    }

    @Override
    protected List<ChannelTask> handleSuccessTask(List<ChannelTask> channelTasks) {
        return channelTasks;
    }
}
