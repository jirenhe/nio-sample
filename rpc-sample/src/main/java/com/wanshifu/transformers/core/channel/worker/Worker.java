package com.wanshifu.transformers.core.channel.worker;

import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;

public interface Worker {

    void init() throws InitException;

    void start();

    void execute(ChannelTask channelTask);

    int getProcessingCount();

    int getFinishCount();

    void shutdown();

    int getId();
}
