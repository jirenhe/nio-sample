package com.wanshifu.transformers.plugin.write.stream;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.plugin.write.Writer;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;

public class StreamWriter extends Configurable implements Writer {

    private BufferedWriter writer;

    @Override
    public void init() {
        writer = new BufferedWriter(new OutputStreamWriter(System.out, StandardCharsets.UTF_8));
    }

    @Override
    public List<ChannelTask> batchWrite(List<ChannelTask> channelTasks) {
        for (ChannelTask channelTask : channelTasks) {
            write(channelTask);
        }
        return Collections.emptyList();
    }

    @Override
    public void write(ChannelTask channelTask) {
        try {
            writer.write(taskToString(channelTask));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String taskToString(ChannelTask channelTask) {
        return channelTask.getRecord().toString();
    }

    @Override
    public void destroy() {
    }

}
