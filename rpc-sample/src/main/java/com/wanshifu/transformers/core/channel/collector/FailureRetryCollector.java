package com.wanshifu.transformers.core.channel.collector;

import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.loadbalance.LoadBalance;
import com.wanshifu.transformers.core.channel.taskpool.TaskPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class FailureRetryCollector extends collector {

    private final LoadBalance loadBalance;

    private final int retryTimes;

    private final static Logger LOGGER = LoggerFactory.getLogger(FailureRetryCollector.class);

    public FailureRetryCollector(TaskPool taskPool, LoadBalance loadBalance, int retryTimes) {
        super(taskPool);
        this.loadBalance = loadBalance;
        this.retryTimes = retryTimes;
    }

    @Override
    protected void postInit() {

    }

    @Override
    protected List<ChannelTask> handleFailureTask(List<ChannelTask> channelTasks) {
        List<ChannelTask> result = new ArrayList<>(channelTasks.size());
        for (ChannelTask channelTask : channelTasks) {
            int taskRetryTimes = channelTask.getRetryTimes();
            if (taskRetryTimes >= retryTimes) {
                LOGGER.error("channelTask retryTimes over times - {}", channelTask.getTaskId());
                result.add(channelTask);
            } else {
                LOGGER.debug("fail channelTask retry - {}", channelTask);
                channelTask.increaseRetryTimes();
                loadBalance.proxy(channelTask);
            }
        }
        return result;
    }

    @Override
    protected List<ChannelTask> handleSuccessTask(List<ChannelTask> channelTasks) {
        return channelTasks;
    }
}
