package com.wanshifu.transformers.core.job;

import com.wanshifu.transformers.common.*;
import com.wanshifu.transformers.common.bean.Task;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.statistics.VMInfo;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.core.alarm.AlarmContainer;
import com.wanshifu.transformers.core.channel.Channel;
import com.wanshifu.transformers.core.channel.ChannelContainer;
import com.wanshifu.transformers.core.job.transport.Transport;
import com.wanshifu.transformers.plugin.reader.Reader;
import com.wanshifu.transformers.plugin.reader.ReaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class JobContainer extends AbstractEventHandleContext implements Container, Transport {

    private Reader reader;

    private List<Channel> channels;

    private State state;

    private String name;

    private Monitor monitor;

    private static final Logger LOGGER = LoggerFactory.getLogger(JobContainer.class);

    private final long startTime = System.currentTimeMillis();

    public JobContainer(Configuration allConfiguration) {
        super(allConfiguration, null);
    }

    @Override
    public void init() throws InitException {
        state = State.INIT;
        Configuration configuration = this.getConfig();
        monitor = new Monitor();
        LOGGER.info("prepare init job config is : {}", configuration.toJSON(true));
        Configuration readerConfig = configuration.getConfiguration(CoreConstants.READER_PLUGIN);
        name = configuration.getNecessaryValue(CoreConstants.NAME);
        List<Configuration> channelConfigurations = configuration.getListConfiguration(CoreConstants.CHANNELS);
        reader = ReaderFactory.getReader(readerConfig);

        AlarmContainer alarmContainer = new AlarmContainer(this);
        reader.init();
        monitor.init();
        alarmContainer.init();
        channels = new ArrayList<>(channelConfigurations.size());
        Set<String> tmp = new HashSet<>();
        for (Configuration channelConfiguration : channelConfigurations) {
            String channelName = channelConfiguration.getNecessaryValue(CoreConstants.NAME);
            if (tmp.contains(channelName)) {
                throw new ConfigurationException("channel name can not duplicate!");
            }
            tmp.add(channelName);
            channelConfiguration.set(CoreConstants.NAME, this.name + "-" + channelName);
            Channel channel = new ChannelContainer(channelConfiguration, this);
            channels.add(channel);
            channel.init();
        }
        LOGGER.info("prepare job init successful! : name is {} reader is : {} channels size is {}", name, reader.getClass().getSimpleName(), channels.size());
    }

    @Override
    public void start() {
        try {
            state = State.RUNNING;
            for (Channel channel : channels) {
                channel.start();
            }
            monitor.printlnVmInfo();
            monitor.startMonitor();
            reader.startReader(this);
        } catch (Exception e) {
            this.publishEvent(new RuntimeExceptionEvent(this, e));
            LOGGER.error("exception happened!", e);
            this.shutdown();
        }
    }

    @Override
    public void send(Task task) {
        int failCount = 0;
        for (Channel channel : channels) {
            try {
                channel.accept(task);
            } catch (Exception e) {
                LOGGER.error("channel is unavailable!", e);
                failCount++;
            }
        }
        if (failCount == channels.size()) {
            throw new RuntimeException();
        }
    }

    @Override
    public void done() {
        this.shutdown();
        LOGGER.info("job : {} done take time : {}", getName(), System.currentTimeMillis() - startTime);
    }

    @Override
    public void shutdown() {
        if (State.SHUTDOWN != state) {
            LOGGER.info("job : {} shutdown now", getName());
            state = State.SHUTDOWN;
            reader.destroy();
            for (Channel channel : channels) {
                channel.shutdown();
            }
            monitor.print();
            monitor.printlnVmInfo();
            monitor.shutdown();
            LOGGER.info("job : {} shutdown successful!", getName());
        }
    }

    @Override
    public String monitorInfo() {
        StringBuilder stringBuilder = new StringBuilder("job :").append(getName()).append("trace monitorInfo:\r\n\r\n");
        for (Channel channel : channels) {
            stringBuilder.append(channel.monitorInfo()).append("\r\n");
        }
        return stringBuilder.toString();
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public State getState() {
        return state;
    }

    private class Monitor implements Runnable {

        private Thread runner;

        private long lastVMTime;

        private static final long ANALYZE_INTERVAL = 5000; // 打印间隔，5秒一次

        private static final long VM_PRINT_INTERVAL = 300000; // VM信息输出间隔，5分钟一次

        final VMInfo vmInfo = VMInfo.getInstance();

        private final Logger MONITOR_LOGGER = LoggerFactory.getLogger(Monitor.class);

        public void init() {
            runner = new Thread(this, getName() + "Monitor");
            LOGGER.info("{} monitor init", getName());
        }

        public void startMonitor() {
            lastVMTime = System.currentTimeMillis();
            runner.start();
        }

        public void shutdown() {
            runner.interrupt();
        }

        @Override
        public void run() {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Thread.sleep(ANALYZE_INTERVAL);
                    print();
                } catch (InterruptedException e) {
                    if (state == State.SHUTDOWN) {
                        MONITOR_LOGGER.info("Monitor shutdown");
                    } else {
                        MONITOR_LOGGER.error("Monitor unexpected interrupted!", e);
                    }
                    return;
                }
            }
        }

        private void print() {
            MONITOR_LOGGER.info(monitorInfo());
            long now = System.currentTimeMillis();
            long vmTimeDiff = -lastVMTime;
            if (vmTimeDiff >= VM_PRINT_INTERVAL) {
                printlnVmInfo();
                lastVMTime = now;
            }
        }

        private void printlnVmInfo() {
            MONITOR_LOGGER.info(vmInfo.getDelta());
        }
    }

}
