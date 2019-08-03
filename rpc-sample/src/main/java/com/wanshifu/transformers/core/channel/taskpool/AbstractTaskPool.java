package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.ContextAware;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.taskpool.event.PersistentFailEvent;
import com.wanshifu.transformers.core.channel.taskpool.event.TaskDePoolEvent;
import com.wanshifu.transformers.core.channel.taskpool.event.TaskEnPoolEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractTaskPool extends ContextAware implements TaskPool {

    protected static final Logger LOGGER = LoggerFactory.getLogger(AbstractTaskPool.class);

    protected AbstractTaskPool(Context context) {
        this.setContext(context);
    }

    @Override
    public boolean offer(ChannelTask channelTask) {
        long time = System.currentTimeMillis();
        try {
            enPool(channelTask);
            getContext().publishEvent(new TaskEnPoolEvent(this, this.size()));
        } catch (TaskPersistentException e) {
            persistentFail(e);
        }
        LOGGER.trace("offer take time : {}", System.currentTimeMillis() - time);
        return true;
    }

    @Override
    public boolean take(ChannelTask channelTask) {
        try {
            dePool(channelTask);
            getContext().publishEvent(new TaskDePoolEvent(this, this.size()));
        } catch (TaskPersistentException e) {
            persistentFail(e);
        }
        return true;
    }

    private void persistentFail(TaskPersistentException e) {
        LOGGER.error("task persistent fail!", e);
        getContext().publishEvent(new PersistentFailEvent(this, e));
    }

    protected abstract void enPool(ChannelTask channelTask) throws TaskPersistentException;

    protected abstract void dePool(ChannelTask channelTask) throws TaskPersistentException;

}
