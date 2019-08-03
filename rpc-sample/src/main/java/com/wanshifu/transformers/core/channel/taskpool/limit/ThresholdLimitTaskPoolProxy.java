package com.wanshifu.transformers.core.channel.taskpool.limit;

import com.wanshifu.transformers.common.ContextAware;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.taskpool.TaskPool;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.core.channel.taskpool.event.LimitTriggerEvent;

import java.util.Collection;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ThresholdLimitTaskPoolProxy extends ContextAware implements TaskPool {

    private final int threshold;

    private final ReentrantLock lock;

    private final Condition condition;

    private final TaskPool taskPool;

    public ThresholdLimitTaskPoolProxy(TaskPool taskPool, int threshold, boolean fair) {
        this.threshold = threshold;
        lock = new ReentrantLock(fair);
        condition = lock.newCondition();
        this.taskPool = taskPool;
    }

    private void acquire() {
        lock.lock();
        try {
            while (taskPool.size() >= threshold) {
                getContext().publishEvent(new LimitTriggerEvent(this));
                try {
                    condition.await();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } finally {
            lock.unlock();
        }
    }

    private void release() {
        if (taskPool.size() < threshold) {
            lock.lock();
            try {
                condition.signalAll();
            } finally {
                lock.unlock();
            }
        }
    }


    @Override
    public void init() throws InitException {
        taskPool.init();
    }

    @Override
    public boolean offer(ChannelTask channelTask) {
        acquire();
        return taskPool.offer(channelTask);
    }

    @Override
    public boolean take(ChannelTask channelTask) {
        boolean result = taskPool.take(channelTask);
        release();
        return result;
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
