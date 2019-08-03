package com.wanshifu.transformers.core.channel.loadbalance;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.utils.queue.ArrayRingQueue;
import com.wanshifu.transformers.common.utils.queue.RingQueue;
import com.wanshifu.transformers.core.channel.worker.Worker;

import java.util.List;

/**
 * 轮询负载，通过环形队列实现。负载最平均
 */
public class PollingBalance extends LoadBalance {

    private final RingQueue<Worker> ringWorkers;

    public PollingBalance(Context context, List<Worker> workers) {
        super(context, workers);
        this.ringWorkers = new ArrayRingQueue<>(workers);
    }

    @Override
    public void proxy(ChannelTask channelTask) {
        ringWorkers.getAndMove().execute(channelTask);
    }
}
