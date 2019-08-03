package com.wanshifu.transformers.core.channel.collector;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.ContextAware;
import com.wanshifu.transformers.common.EventListener;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.taskpool.TaskPool;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteFailEvent;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteSuccessEvent;

import java.util.List;

public abstract class collector extends ContextAware {

    private final TaskPool taskPool;

    protected collector(TaskPool taskPool) {
        this.taskPool = taskPool;
    }

    public void init() {
        Context context = this.getContext();
        context.registerEventListener(new FailureListener());
        context.registerEventListener(new SuccessListener());
        postInit();
    }

    protected abstract void postInit();

    protected abstract List<ChannelTask> handleFailureTask(List<ChannelTask> channelTasks);

    protected abstract List<ChannelTask> handleSuccessTask(List<ChannelTask> channelTasks);

    private class FailureListener implements EventListener<TaskExecuteFailEvent> {

        @Override
        public void onEvent(TaskExecuteFailEvent event) {
            List<ChannelTask> channelTasks = handleFailureTask(event.getData());
            for (ChannelTask channelTask : channelTasks) {
                taskPool.take(channelTask);
            }
        }
    }

    private class SuccessListener implements EventListener<TaskExecuteSuccessEvent> {

        @Override
        public void onEvent(TaskExecuteSuccessEvent event) {
            List<ChannelTask> channelTasks = handleSuccessTask(event.getData());
            for (ChannelTask channelTask : channelTasks) {
                taskPool.take(channelTask);
            }
        }
    }

}
