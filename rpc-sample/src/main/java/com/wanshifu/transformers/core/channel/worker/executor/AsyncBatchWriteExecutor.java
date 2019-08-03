package com.wanshifu.transformers.core.channel.worker.executor;

import com.wanshifu.transformers.common.*;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.plugin.write.Writer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class AsyncBatchWriteExecutor extends AbstractBatchWriteExecutor implements WriteExecutor, Runnable {

    private Thread runner;

    private final Lock lock = new ReentrantLock();

    private final Condition notEmpty = lock.newCondition();

    private final Condition notFull = lock.newCondition();

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncBatchWriteExecutor.class);

    public AsyncBatchWriteExecutor(Context context, int id, Writer writer) {
        super(context, id, writer);
    }


    @Override
    protected void postInit() {
        Context context = getContext();
        runner = new Thread(this, context.getName() + " WriteExecutor-" + id);
        LOGGER.info("AsyncBatchWriteExecutor id {} init buffer size is {} polling time is {} second", id, capacity, pollingTime);
    }

    @Override
    public void start() {
        runner.start();
    }

    @Override
    public void shutdown() {
        if (getContext().getState() != State.SHUTDOWN) {
            throw new IllegalStateException();
        }
        lock.lock();
        try {
            doFlush();
            notEmpty.signalAll();
        } finally {
            lock.unlock();
            writer.destroy();
        }
        LOGGER.info("AsyncBatchWriteExecutor id : {} shutdown", id);
    }

    @Override
    public void sendToWrite(ChannelTask channelTask) {
        lock.lock();
        try {
            while (buffer.size() + 1 >= capacity) {
                LOGGER.debug("buffer capacity arrived");
                try {
                    notEmpty.signalAll();
                    notFull.await();
                } catch (InterruptedException e) {
                    getContext().publishEvent(new RuntimeExceptionEvent(this, new UnexpectedException("unexpected interrupted happened! on WriteExecutor-" + id, e)));
                }
            }
            buffer.add(channelTask);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public void run() {
        for (; ; ) {
            lock.lock();
            try {
                if (getContext().getState() == State.SHUTDOWN && buffer.size() == 0) {
                    return;
                }
                doFlush();
                notFull.signalAll();
                if (pollingTime > 0) {
                    notEmpty.await(pollingTime, TimeUnit.SECONDS);
                } else {
                    notEmpty.await();
                }
            } catch (InterruptedException e) {
                if (getContext().getState() != State.SHUTDOWN) {
                    getContext().publishEvent(new RuntimeExceptionEvent(this, new UnexpectedException("unexpected interrupted happened! on WriteExecutor-" + id, e)));
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
