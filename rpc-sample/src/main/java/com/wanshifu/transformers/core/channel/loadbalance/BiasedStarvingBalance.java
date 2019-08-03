package com.wanshifu.transformers.core.channel.loadbalance;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.worker.Worker;

import java.util.List;
import java.util.Objects;

/**
 * 饥饿线程优先策略，吞吐量最低差。
 */
public class BiasedStarvingBalance extends LoadBalance {

    public BiasedStarvingBalance(Context context, List<Worker> workers) {
        super(context, workers);
    }

    @Override
    public void proxy(ChannelTask channelTask) {
        int leastSize = Integer.MAX_VALUE;
        Worker target = null;
        for (Worker worker : workers) {
            int size = worker.getProcessingCount();
            if (size == 0) {
                worker.execute(channelTask);
                return;
            } else if (size <= leastSize) {
                leastSize = size;
                target = worker;
            }
        }
        Objects.requireNonNull(target).execute(channelTask);
    }

}
