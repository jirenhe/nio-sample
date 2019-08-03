package com.wanshifu.transformers.plugin.write.none;

import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.plugin.write.Writer;

import java.util.Collections;
import java.util.List;

public class NoOpWriter implements Writer {

    @Override
    public void init() {

    }

    @Override
    public List<ChannelTask> batchWrite(List<ChannelTask> channelTasks) {
        return Collections.emptyList();
    }

    @Override
    public void write(ChannelTask channelTask) {

    }

    @Override
    public void destroy() {

    }
}
