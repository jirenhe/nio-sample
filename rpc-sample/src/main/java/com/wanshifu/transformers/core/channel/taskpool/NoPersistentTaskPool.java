package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.bean.ChannelTask;

import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class NoPersistentTaskPool extends AbstractTaskPool implements TaskPool {

    private final AtomicInteger atomicInteger = new AtomicInteger(0);

    public NoPersistentTaskPool(Context channelContext) {
        super(channelContext);
    }

    @Override
    protected void enPool(ChannelTask channelTask) {
        atomicInteger.incrementAndGet();
    }

    @Override
    protected void dePool(ChannelTask channelTask) {
        atomicInteger.decrementAndGet();
    }

    @Override
    public int size() {
        return atomicInteger.get();
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Collection<ChannelTask> getAll() {
        return Collections.emptyList();
    }

    @Override
    public void init() {

    }

}
