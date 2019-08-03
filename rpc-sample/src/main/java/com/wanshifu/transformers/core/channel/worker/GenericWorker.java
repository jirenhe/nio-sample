package com.wanshifu.transformers.core.channel.worker;

import com.wanshifu.transformers.common.*;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteFailEvent;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteSuccessEvent;
import com.wanshifu.transformers.core.channel.worker.executor.AsyncBatchWriteExecutor;
import com.wanshifu.transformers.core.channel.worker.executor.SyncBatchWriteExecutor;
import com.wanshifu.transformers.core.channel.worker.executor.SyncWriteExecutor;
import com.wanshifu.transformers.core.channel.worker.executor.WriteExecutor;
import com.wanshifu.transformers.core.channel.worker.transformer.Transformer;
import com.wanshifu.transformers.core.channel.worker.transformer.TransformerLoader;
import com.wanshifu.transformers.plugin.write.Writer;
import com.wanshifu.transformers.plugin.write.WriterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class GenericWorker extends ContextAware implements Worker, Runnable {

    private final int id;

    private Thread runner;

    private BlockingQueue<ChannelTask> channelTaskQueue;

    private List<Transformer> transformerList;

    private WriteExecutor writeExecutor;

    private int finishCount = 0;

    private final static Logger LOGGER = LoggerFactory.getLogger(GenericWorker.class);

    private final static int INIT = 0;

    private final static int TAKING = 1;

    private final static int PROCESSING = 2;

    private final static int DONE = 3;

    private volatile int state = INIT;

    public GenericWorker(Context context, int id) {
        this.id = id;
        this.setContext(context);
    }

    @Override
    public void init() throws InitException {
        Context context = this.getContext();
        Configuration configuration = context.getConfig();
        channelTaskQueue = new LinkedBlockingQueue<>(Integer.MAX_VALUE);
        String type = configuration.getString(CoreConstants.WORKER_WRITE_EXECUTOR_TYPE);
        Writer writer = WriterFactory.getWrite(configuration.getConfiguration(CoreConstants.WORKER_WRITE_PLUGIN));
        LOGGER.info("load writer plugin is : {}", writer.getClass().getSimpleName());
        writeExecutor = chooseExecutor(type, id, context, writer);
        StringBuilder transformerInfo = new StringBuilder();
        transformerList = TransformerLoader.loadTransForm(configuration.getListConfiguration(CoreConstants.WORKER_TRANSFORMER));
        for (Transformer transformer : transformerList) {
            transformerInfo.append(transformer.getClass().getSimpleName()).append(",");
            transformer.init();
        }
        writeExecutor.init();
        runner = new Thread(this, context.getName() + " GenericWorker-" + id);

        LOGGER.info("GenericWorker id : {} init successful! using writeExecutor is {} transformer monitorInfo : {}",
                id, writeExecutor.getClass().getSimpleName(), transformerInfo.toString());
    }

    @Override
    public void start() {
        runner.start();
        writeExecutor.start();
    }

    @Override
    public void execute(ChannelTask channelTask) {
        try {
            channelTaskQueue.put(channelTask);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public int getProcessingCount() {
        return this.channelTaskQueue.size();
    }

    @Override
    public int getFinishCount() {
        return finishCount;
    }

    @Override
    public void shutdown() {
        if (state == TAKING) {
            runner.interrupt();
        }
        synchronized (this) {
            while (state != DONE) {
                try {
                    this.wait();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        writeExecutor.shutdown();
        for (Transformer transformer : transformerList) {
            transformer.shutdown();
        }
        LOGGER.info("GenericWorker id : {} showdown", id);
    }

    @Override
    public int getId() {
        return id;
    }

    /**
     * you will never understand this logic, because I don't understand this myself, either :)
     */
    @Override
    public void run() {
        ChannelTask channelTask = null;
        Context context = getContext();
        for (; ; ) {
            try {
                channelTask = channelTaskQueue.poll(50, TimeUnit.MILLISECONDS);
                if (channelTask != null) {
                    state = PROCESSING;
                    doExecute(channelTask, context);
                } else {
                    if (State.SHUTDOWN == context.getState()) {
                        done();
                        return;
                    } else {
                        if (Thread.currentThread().isInterrupted()) {
                            List<ChannelTask> tmp = new ArrayList<>(channelTaskQueue.size());
                            channelTaskQueue.drainTo(tmp);
                            state = PROCESSING;
                            for (ChannelTask channelTask1 : tmp) {
                                doExecute(channelTask1, context);
                            }
                        } else {
                            state = TAKING;
                            channelTask = channelTaskQueue.take();
                        }
                        if (channelTask != null) {
                            state = PROCESSING;
                            doExecute(channelTask, context);
                        }
                    }
                }
            } catch (InterruptedException e) {
                if (State.SHUTDOWN != getContext().getState()) {
                    context.publishEvent(new RuntimeExceptionEvent(this, new UnexpectedException("unexpected interrupted happened! on worker-" + id, e)));
                } else {
                    Thread.currentThread().interrupt();
                    LOGGER.info("worker process interrupt! could be response shutdown");
                }
                done();
                return;
            } catch (Exception e) {
                if (channelTask != null) {
                    LOGGER.error("channelTask fail", e);
                    context.publishEvent(new TaskExecuteFailEvent(this, Collections.singletonList(channelTask),e));
                }
            }
        }
    }

    private void done() {
        synchronized (this) {
            state = DONE;
            this.notifyAll();
        }
    }

    private void doExecute(ChannelTask channelTask, Context context) {
        if (channelTask.getRetryTimes() == 0) {
            doTransform(channelTask);
        }
        if (channelTask.getRecord() != null) {
            writeExecutor.sendToWrite(channelTask);
        } else {
            context.publishEvent(new TaskExecuteSuccessEvent(this, Collections.singletonList(channelTask)));
        }
        finishCount++;
    }

    private void doTransform(ChannelTask channelTask) {
        if (transformerList.size() > 0) {
            for (Transformer transformer : transformerList) {
                transformer.doTransform(channelTask);
            }
        }
    }

    private WriteExecutor chooseExecutor(String type, int id, Context context, Writer writer) {
        switch (type) {
            case "syncBatch":
                return new SyncBatchWriteExecutor(context, id, writer);
            case "asyncBatch":
                return new AsyncBatchWriteExecutor(context, id, writer);
            default:
                return new SyncWriteExecutor(context, id, writer);
        }
    }

}
