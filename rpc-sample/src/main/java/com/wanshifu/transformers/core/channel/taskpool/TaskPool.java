package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;

import java.util.Collection;

public interface TaskPool {

    void init() throws InitException;

    boolean offer(ChannelTask channelTask);

    boolean take(ChannelTask channelTask);

    int size();

    void shutdown();

    Collection<ChannelTask> getAll();
}
