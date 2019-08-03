package com.wanshifu.transformers.core.channel.loadbalance;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.worker.Worker;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * 随机负载，吞吐量最高
 */
public class RandomBalance extends LoadBalance {

    private final int bound;

    public RandomBalance(Context context, List<Worker> workers) {
        super(context, workers);
        bound = workers.size();
    }

    @Override
    public void proxy(ChannelTask channelTask) {
        workers.get(ThreadLocalRandom.current().nextInt(0, bound)).execute(channelTask);
    }
}
