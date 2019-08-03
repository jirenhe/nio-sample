package com.wanshifu.transformers.core.channel.worker.executor;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteFailEvent;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteSuccessEvent;
import com.wanshifu.transformers.plugin.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class SyncWriteExecutor extends AbstractWriteExecutor implements WriteExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncBatchWriteExecutor.class);

    public SyncWriteExecutor(Context context, int id, Writer writer) {
        super(context, id, writer);
    }

    @Override
    public void init() throws InitException {
        writer.init();
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {
        writer.destroy();
    }

    @Override
    public void sendToWrite(ChannelTask channelTask) {
        long time = System.currentTimeMillis();
        try {
            writer.write(channelTask);
            getContext().publishEvent(new TaskExecuteSuccessEvent(this, Collections.singletonList(channelTask)));
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("writer channelTask id : {} take time : {}", channelTask.getTaskId(), System.currentTimeMillis() - time);
            }
        } catch (Exception e) {
            LOGGER.error("channelTask execute fail! - {}", channelTask, e);
            getContext().publishEvent(new TaskExecuteFailEvent(this, Collections.singletonList(channelTask),e));
        }
    }
}
