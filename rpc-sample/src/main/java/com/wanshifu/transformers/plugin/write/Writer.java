package com.wanshifu.transformers.plugin.write;

import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;

import java.util.List;

public interface Writer {

    void init() throws InitException;

    List<ChannelTask> batchWrite(List<ChannelTask> channelTasks) throws WriteException;

    void write(ChannelTask channelTask) throws WriteException;

    void destroy();
}
