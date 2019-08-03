package com.wanshifu.transformers.core.channel.worker.transformer;

import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;

public interface Transformer {

    void init() throws InitException;

    void shutdown();

    void doTransform(ChannelTask channelTask);
}
