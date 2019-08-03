package com.wanshifu.transformers.core.channel.worker.executor;

import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;

public interface WriteExecutor {

    void init() throws InitException;

    void start();

    void shutdown();

    void sendToWrite(ChannelTask channelTask);
}
