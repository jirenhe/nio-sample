package com.wanshifu.transformers.core.channel.taskpool.limit;

import com.wanshifu.transformers.common.ContextAware;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.taskpool.TaskPool;
import com.wanshifu.transformers.core.channel.taskpool.event.LimitTriggerEvent;

import java.util.Collection;
import java.util.concurrent.Semaphore;

/**
 * 没有存在的价值
 */
@Deprecated
public class SemaphoreLimitTaskPoolProxy extends ContextAware implements TaskPool {

    private final Semaphore semaphore;

    private final TaskPool taskPool;

    public SemaphoreLimitTaskPoolProxy(TaskPool taskPool, int count, boolean fair) {
        semaphore = new Semaphore(count, fair);
        this.taskPool = taskPool;
    }

    private void acquire() {
        if (semaphore.availablePermits() == 0) {
            getContext().publishEvent(new LimitTriggerEvent(this));
        }
        semaphore.acquireUninterruptibly();
    }

    private void release() {
        semaphore.release();
    }

    @Override
    public void init() throws InitException {
        taskPool.init();
    }

    @Override
    public boolean offer(ChannelTask channelTask) {
        acquire();
        boolean result = taskPool.offer(channelTask);
        release();
        return result;
    }

    @Override
    public boolean take(ChannelTask channelTask) {
        return taskPool.take(channelTask);
    }

    @Override
    public int size() {
        return taskPool.size();
    }

    @Override
    public void shutdown() {
        taskPool.shutdown();
    }

    @Override
    public Collection<ChannelTask> getAll() {
        return taskPool.getAll();
    }
}
