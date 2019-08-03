package com.wanshifu.transformers.core.channel.loadbalance;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.ContextAware;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.worker.Worker;

import java.util.List;

public abstract class LoadBalance extends ContextAware {

    protected final List<Worker> workers;

    public LoadBalance(Context context, List<Worker> workers) {
        setContext(context);
        this.workers = workers;
    }

    public abstract void proxy(ChannelTask channelTask);
}
