package com.wanshifu.transformers.core.channel.worker.executor;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteFailEvent;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteSuccessEvent;
import com.wanshifu.transformers.plugin.write.Writer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.ListUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractBatchWriteExecutor extends AbstractWriteExecutor {

    protected static final Logger LOGGER = LoggerFactory.getLogger(SyncBatchWriteExecutor.class);

    protected List<ChannelTask> buffer;

    protected int capacity;

    protected int pollingTime;

    public AbstractBatchWriteExecutor(Context context, int id, Writer writer) {
        super(context, id, writer);
    }

    @Override
    public void init() throws InitException {
        Context context = this.getContext();
        int bufferSize = context.getConfig().getInt(CoreConstants.WORKER_WRITE_EXECUTOR_BUFFER_SIZE, 32);
        pollingTime = context.getConfig().getInt(CoreConstants.WORKER_WRITE_EXECUTOR_POLLING_TIME, -1);
        capacity = bufferSize;
        buffer = new ArrayList<>(bufferSize);
        writer.init();
        postInit();
        LOGGER.info("AsyncBatchWriteExecutor id {} init buffer size is {} polling time is {} second", id, bufferSize, pollingTime);
    }

    protected abstract void postInit();

    @SuppressWarnings("unchecked")
    protected void doFlush() {
        long time = System.currentTimeMillis();
        List<ChannelTask> copyList = new ArrayList<>(this.buffer);
        try {
            if (copyList.size() == 0) {
                return;
            }
            List<ChannelTask> failureChannelTasks = writer.batchWrite(copyList);
            if (CollectionUtils.isEmpty(failureChannelTasks)) {
                getContext().publishEvent(new TaskExecuteSuccessEvent(this, copyList));
            } else {
                List<ChannelTask> successList = ListUtils.subtract(copyList, failureChannelTasks);
                getContext().publishEvent(new TaskExecuteSuccessEvent(this, successList));
                getContext().publishEvent(new TaskExecuteFailEvent(this, failureChannelTasks,new RuntimeException("写插件出现错误，产生失败事件")));
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch writer task. buffer size : {} take time : {}", this.buffer.size(), System.currentTimeMillis() - time);
            }
        } catch (Exception e) {
            LOGGER.error("tasks batch execute fail!", e);
            getContext().publishEvent(new TaskExecuteFailEvent(this, copyList,e));
        } finally {
            this.buffer.clear();
        }
    }
}
