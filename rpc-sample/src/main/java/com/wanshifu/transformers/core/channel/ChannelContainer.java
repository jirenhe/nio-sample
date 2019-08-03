package com.wanshifu.transformers.core.channel;

import com.wanshifu.transformers.common.*;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.bean.Task;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.core.channel.collector.FailureRetryCollector;
import com.wanshifu.transformers.core.channel.collector.SampleCollector;
import com.wanshifu.transformers.core.channel.collector.collector;
import com.wanshifu.transformers.core.channel.loadbalance.LoadBalance;
import com.wanshifu.transformers.core.channel.loadbalance.LoadBalanceFactory;
import com.wanshifu.transformers.core.channel.taskpool.TaskPool;
import com.wanshifu.transformers.core.channel.taskpool.TaskPoolFactory;
import com.wanshifu.transformers.core.channel.taskpool.event.LimitTriggerEvent;
import com.wanshifu.transformers.core.channel.taskpool.event.TaskDePoolEvent;
import com.wanshifu.transformers.core.channel.taskpool.event.TaskEnPoolEvent;
import com.wanshifu.transformers.core.channel.worker.GenericWorker;
import com.wanshifu.transformers.core.channel.worker.Worker;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteFailEvent;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteSuccessEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;

public class ChannelContainer extends AbstractEventHandleContext implements Channel {

    private TaskPool taskPool;

    private LoadBalance loadBalance;

    private List<Worker> workers;

    private collector collector;

    private String name;

    private boolean monitorWorkerDetail;

    private final Monitor monitor = new Monitor();

    private Collection<ChannelTask> recoverChannelTasks; //启动时任务池恢复的任务

    private volatile State state;

    private static final Logger LOGGER = LoggerFactory.getLogger(ChannelContainer.class);

    public ChannelContainer(Configuration configuration,Context parent) {
        super(configuration, parent);
    }

    @Override
    public void init() throws InitException {
        state = State.INIT;
        Configuration configuration = this.getConfig();
        this.name = configuration.getNecessaryValue(CoreConstants.NAME);
        monitorWorkerDetail = configuration.getBool(CoreConstants.CHANNEL_MONITOR_WORKER_DETAIL, false);
        int workerSize = configuration.getInt(CoreConstants.CHANNEL_WORK_SIZE, Runtime.getRuntime().availableProcessors());
        int retryTimes = configuration.getInt(CoreConstants.CHANNEL_RETRY_TIMES, 0);
        workers = new ArrayList<>(workerSize);
        for (int i = 0; i < workerSize; i++) {
            Worker worker = new GenericWorker(this, i);
            worker.init();
            workers.add(worker);
        }
        taskPool = TaskPoolFactory.getTaskPool(this);
        loadBalance = LoadBalanceFactory.getLoadBalance(this, workers);
        if (retryTimes > 0) {
            collector = new FailureRetryCollector(taskPool, loadBalance, retryTimes);
        } else {
            collector = new SampleCollector(taskPool);
        }
        collector.setContext(this);
        taskPool.init();
        collector.init();
        monitor.init();
        LOGGER.info("channelContainer name : {} init successful! worker size is {} using task pool is {} using load balance is {} using collector is {}",
                name,
                workerSize,
                taskPool.getClass().getSimpleName(),
                loadBalance.getClass().getSimpleName(),
                collector.getClass().getSimpleName()
        );
        recoverChannelTasks = taskPool.getAll();
    }

    @Override
    public void start() {
        LOGGER.info("channelContainer name : {} start!", name);
        state = State.RUNNING;
        for (Worker worker : workers) {
            worker.start();
        }
        LOGGER.info("re-deliver task size : {} ", recoverChannelTasks.size());
        //恢复的任务需要第一时间进队列
        for (ChannelTask channelTask : recoverChannelTasks) {
            loadBalance.proxy(channelTask);
        }
        recoverChannelTasks = null;//help gc
    }

    @Override
    public void shutdown() {
        state = State.SHUTDOWN;
        for (Worker worker : workers) {
            worker.shutdown();
        }
        taskPool.shutdown();
    }

    @Override
    public String monitorInfo() {
        return monitor.doAnalyze();
    }

    @Override
    public void accept(Task task) {
        if (state == State.RUNNING) {
            ChannelTask channelTask = new ChannelTask(task, UUID.randomUUID().toString());
            taskPool.offer(channelTask);
            loadBalance.proxy(channelTask);
        } else {
            throw new IllegalStateException("state must be running can accept channelTask!");
        }
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public State getState() {
        return state;
    }

    private class Monitor {

        private final long startTime = System.currentTimeMillis();

        private long lastAnalyzeTime = System.currentTimeMillis();

        private final LongAdder atomicLimitTriggerTimes = new LongAdder();

        private final LongAdder atomicTaskEnPoolTimes = new LongAdder();

        private final LongAdder atomicTaskDePoolTimes = new LongAdder();

        private final LongAdder atomicFailureTaskTimes = new LongAdder();

        private final LongAdder atomicAllFinishTask = new LongAdder();

        private final Logger MONITOR_LOGGER = LoggerFactory.getLogger(ChannelContainer.class);

        public void init() {
            registerEventListener(new LimitTriggerEventListener());
            registerEventListener(new TaskDePoolEventListener());
            registerEventListener(new TaskEnPoolEventListener());
            registerEventListener(new TaskExecuteFailEventListener());
            registerEventListener(new TaskExecuteSuccessEventListener());
            registerEventListener(new RuntimeExceptionEventListener());
            LOGGER.info("{} monitor init", getName());
        }

        private String doAnalyze() {
            long limitTriggerTimes, taskEnPoolTimes, taskDePoolTimes, failureTaskTimes, totalFinishCount;
            int poolSize = taskPool.size();
            long now = System.currentTimeMillis();
            double seconds = (double) (now - lastAnalyzeTime) / 1000L;
            double totalSeconds = (double) (now - startTime) / 1000L;
            limitTriggerTimes = atomicLimitTriggerTimes.sumThenReset();
            taskEnPoolTimes = atomicTaskEnPoolTimes.sumThenReset();
            taskDePoolTimes = atomicTaskDePoolTimes.sumThenReset();
            failureTaskTimes = atomicFailureTaskTimes.sumThenReset();
            totalFinishCount = atomicAllFinishTask.sum();
            double enPoolPerSeconds = taskEnPoolTimes / seconds;
            double dePoolPerSeconds = taskDePoolTimes / seconds;
            double limitTriggerPerSeconds = limitTriggerTimes / seconds;
            double failureTaskPerSeconds = failureTaskTimes / seconds;
            double finishTaskPerSeconds = totalFinishCount / totalSeconds;
            StringBuilder stringBuilder = new StringBuilder("channel : ").append(getName()).append(" trace monitorInfo:\r\n");
            stringBuilder.append(" current task pool size : ").append(poolSize).append(" |");
            stringBuilder.append(" task en pool per second : ").append(toFixed(enPoolPerSeconds, 2)).append(" |");
            stringBuilder.append(" task de pool per second : ").append(toFixed(dePoolPerSeconds, 2)).append(" |");
            stringBuilder.append(" limit trigger per seconds : ").append(toFixed(limitTriggerPerSeconds, 2)).append(" |");
            stringBuilder.append(" failure task per seconds : ").append(toFixed(failureTaskPerSeconds, 2)).append(" |");
            stringBuilder.append(" total finish count : ").append(totalFinishCount).append(" |");
            stringBuilder.append(" finish task per seconds : ").append(toFixed(finishTaskPerSeconds, 2)).append("\r\n");
            if (monitorWorkerDetail) {
                stringBuilder.append(" worker monitorInfo : \r\n");
            }
            for (Worker worker : workers) {
                int finishCount = worker.getFinishCount();
                if (monitorWorkerDetail) {
                    stringBuilder.append("worker id : ")
                            .append(worker.getId())
                            .append(" finish count : ")
                            .append(finishCount)
                            .append(" processing count : ")
                            .append(worker.getProcessingCount())
                            .append("\r\n");
                }
            }
            lastAnalyzeTime = now;
            return stringBuilder.toString();
        }

        private String toFixed(double number, int fixedLength) {
            String str = number + "";
            int index = str.indexOf(".");
            if (index > 0) {
                int rangeLength = index + fixedLength + 1;
                if (rangeLength > str.length()) {
                    return str;
                } else {
                    return str.substring(0, rangeLength);
                }
            } else {
                return str;
            }
        }


        private class LimitTriggerEventListener implements EventListener<LimitTriggerEvent> {

            @Override
            public void onEvent(LimitTriggerEvent event) {
                atomicLimitTriggerTimes.increment();
            }
        }

        private class TaskDePoolEventListener implements EventListener<TaskDePoolEvent> {

            @Override
            public void onEvent(TaskDePoolEvent event) {
                atomicTaskDePoolTimes.increment();
                atomicAllFinishTask.increment();
            }
        }

        private class TaskEnPoolEventListener implements EventListener<TaskEnPoolEvent> {

            @Override
            public void onEvent(TaskEnPoolEvent event) {
                atomicTaskEnPoolTimes.increment();
            }
        }

        private class TaskExecuteFailEventListener implements EventListener<TaskExecuteFailEvent> {

            @Override
            public void onEvent(TaskExecuteFailEvent event) {
                int size = event.getData().size();
                atomicFailureTaskTimes.add(size);
            }
        }

        private class TaskExecuteSuccessEventListener implements EventListener<TaskExecuteSuccessEvent> {

            @Override
            public void onEvent(TaskExecuteSuccessEvent event) {

            }
        }

        private class RuntimeExceptionEventListener implements EventListener<RuntimeExceptionEvent> {

            @Override
            public void onEvent(RuntimeExceptionEvent event) {
                Exception exception = event.getData();
                boolean isShutdown = event.isShutdown();
                MONITOR_LOGGER.error("catch runtime exception isShutdown : {}", isShutdown, exception);
                if (isShutdown) {
                    ChannelContainer.this.shutdown();
                }
            }
        }
    }
}
