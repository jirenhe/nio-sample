package com.wanshifu.transformers.plugin.reader.cannal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.alibaba.otter.canal.protocol.exception.CanalClientException;
import com.wanshifu.transformers.common.utils.StringUtils;
import org.apache.commons.collections.CollectionUtils;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;

public class CanalConnectorCollection implements CanalConnector {

    private final List<ConnectorRunner> connectorRunners;

    public CanalConnectorCollection() {
        connectorRunners = new ArrayList<>();
    }

    public CanalConnectorCollection(int initialCapacity) {
        connectorRunners = new ArrayList<>(initialCapacity);
    }

    public void add(ConnectorRunner connectorRunner) {
        connectorRunners.add(connectorRunner);
    }

    public void add(CanalMetaData canalMetaData) {
        CanalConnector connector = CanalConnectors.newSingleConnector(
                new InetSocketAddress(canalMetaData.getHost(), canalMetaData.getPort()),
                canalMetaData.getDestination(),
                canalMetaData.getUserName(),
                canalMetaData.getPassword());
        this.add(new ConnectorRunner(connector, canalMetaData));

    }

    @Override
    public void connect() throws CanalClientException {
        for (ConnectorRunner connectorRunner : connectorRunners) {
            connectorRunner.connector.connect();
        }
    }

    @Override
    public void disconnect() throws CanalClientException {
        for (ConnectorRunner connectorRunner : connectorRunners) {
            connectorRunner.flag = false;
            synchronized (connectorRunner.lock) {
                connectorRunner.connector.disconnect();
            }
        }
    }

    @Override
    public boolean checkValid() throws CanalClientException {
        for (ConnectorRunner connectorRunner : connectorRunners) {
            if (!connectorRunner.connector.checkValid()) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void subscribe(String filter) throws CanalClientException {
        for (ConnectorRunner connectorRunner : connectorRunners) {
            String f = connectorRunner.canalMetaData.getDataBaseTable();
            if (StringUtils.isEmpty(f)) {
                connectorRunner.connector.subscribe(filter);
            } else {
                connectorRunner.connector.subscribe(f);
            }
        }
    }

    @Override
    public void subscribe() throws CanalClientException {
        for (ConnectorRunner connectorRunner : connectorRunners) {
            String f = connectorRunner.canalMetaData.getDataBaseTable();
            if (StringUtils.isNotEmpty(f)) {
                connectorRunner.connector.subscribe(f);
            } else {
                connectorRunner.connector.subscribe();
            }
        }
    }

    @Override
    public void unsubscribe() throws CanalClientException {
        for (ConnectorRunner connectorRunner : connectorRunners) {
            connectorRunner.connector.unsubscribe();
        }
    }

    @Override
    public Message get(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Message getWithoutAck(int batchSize) throws CanalClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public Message getWithoutAck(int batchSize, Long timeout, TimeUnit unit) throws CanalClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void ack(long batchId) throws CanalClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback(long batchId) throws CanalClientException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void rollback() throws CanalClientException {
        for (ConnectorRunner connectorRunner : connectorRunners) {
            connectorRunner.connector.rollback();
        }
    }

    @Override
    public Message get(int index) {
        throw new UnsupportedOperationException();
    }

    public int size() {
        return connectorRunners.size();
    }

    public void startFetch(BiConsumer<List<CanalEntry.Entry>, CanalMetaData> entryConsumer) {
        if (entryConsumer == null) {
            throw new NullPointerException();
        }
        for (ConnectorRunner connectorRunner : connectorRunners) {
            connectorRunner.startFetch(entryConsumer);
        }
    }

    public void shutdown() {
        this.disconnect();
    }

    private class ConnectorRunner implements Runnable {

        private final CanalConnector connector;

        private final CanalMetaData canalMetaData;

        private final Object lock = new Object();

        private volatile boolean flag = true;

        private final Thread thread;

        private BiConsumer<List<CanalEntry.Entry>, CanalMetaData> hook;

        private ConnectorRunner(CanalConnector connector, CanalMetaData canalMetaData) {
            this.connector = connector;
            this.canalMetaData = canalMetaData;
            thread = new Thread(this);
        }

        private void startFetch(BiConsumer<List<CanalEntry.Entry>, CanalMetaData> entryConsumer) {
            hook = entryConsumer;
            thread.start();
        }

        @Override
        public void run() {
            int count = 0;
            int batchSize = canalMetaData.getBatchSize();
            while (flag) {
                synchronized (lock) {
                    Message message = connector.getWithoutAck(batchSize); // 非阻塞获取指定数量的数据
                    List<CanalEntry.Entry> entryList = message.getEntries();

                    if (CollectionUtils.isEmpty(entryList) && ++count >= 10) {//连续10次以上没有增量数据采用阻塞拉取策略
                        long time = (count - 9) * 10; //每次递增10毫秒，最大阻塞时间：1秒
                        if (time > 1000) {
                            time = 1000;
                        }
                        connector.ack(message.getId());
                        message = connector.getWithoutAck(batchSize, time, TimeUnit.MILLISECONDS); // 阻塞获取指定数量的数据
                        entryList = message.getEntries();
                    }

                    if (CollectionUtils.isNotEmpty(entryList)) {
                        count = 0; //重置计数
                        hook.accept(entryList, canalMetaData);
                    }
                    connector.ack(message.getId());
                }
            }
        }

    }

}
