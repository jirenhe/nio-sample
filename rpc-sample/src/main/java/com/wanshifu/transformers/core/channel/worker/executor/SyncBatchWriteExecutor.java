package com.wanshifu.transformers.core.channel.worker.executor;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.State;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.plugin.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;

public class SyncBatchWriteExecutor extends AbstractBatchWriteExecutor implements WriteExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SyncBatchWriteExecutor.class);

    public SyncBatchWriteExecutor(Context context, int id, Writer writer) {
        super(context, id, writer);
    }


    @Override
    public void init() throws InitException {
        Context context = this.getContext();
        int bufferSize = context.getConfig().getInt(CoreConstants.WORKER_WRITE_EXECUTOR_BUFFER_SIZE, 32);
        capacity = bufferSize;
        buffer = new ArrayList<>(bufferSize);
        writer.init();

    }

    @Override
    protected void postInit() {
        LOGGER.info("syncBatchWriteExecutor id {} init buffer size is {} polling time is {} second", id, capacity, pollingTime);
    }

    @Override
    public void start() {

    }

    @Override
    public void shutdown() {
        if (getContext().getState() != State.SHUTDOWN) {
            throw new IllegalStateException();
        }
        doFlush();
        writer.destroy();
        LOGGER.info("syncBatchWriteExecutor id : {} shutdown", id);
    }

    @Override
    public void sendToWrite(ChannelTask channelTask) {
        buffer.add(channelTask);
        while (buffer.size() >= capacity) {
            doFlush();
        }
    }
}
