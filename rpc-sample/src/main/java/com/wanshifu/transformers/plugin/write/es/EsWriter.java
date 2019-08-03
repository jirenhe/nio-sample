package com.wanshifu.transformers.plugin.write.es;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.plugin.write.Writer;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;

/**
 * 配置es批处理队列大小
 * PUT http://192.168.1.6:9200/_cluster/settings/
 * {
 * "transient": {
 * "threadpool.bulk.queue_size": 1024
 * }
 * }
 */
public class EsWriter extends Configurable implements Writer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EsWriter.class);

    private static final String INDEX = "index";

    private static final String TYPE = "type";

    private static final String ID_COLUMN = "idColumn";

    private static final String WRITER_MODE = "writerMode";

    private static final String EVENT_PROCESS = "eventProcess";

    private String index;

    private String type;

    private String idColumn;

    private final Map<EventType, EventProcessor> eventMap = new HashMap<>();

    private RestHighLevelClient esClient;

    @Override
    public void init() throws InitException {
        Configuration parameter = getConfiguration().getConfiguration(CoreConstants.PARAMETER);
        index = parameter.getNecessaryValue(INDEX);
        type = parameter.getNecessaryValue(TYPE);
        idColumn = parameter.getNecessaryValue(ID_COLUMN);
        esClient = TransportClientFactory.getTransportClient(parameter);
        Configuration eventProcess = parameter.getNecessaryConfiguration(EVENT_PROCESS);
        initEventProcessMap(eventProcess);
    }

    private void initEventProcessMap(Configuration eventProcess) throws ConfigurationException {
        for (EventType eventType : EventType.values()) {
            Configuration configuration = eventProcess.getConfiguration(eventType.toString());
            if (configuration != null) {
                String mode = configuration.getNecessaryValue(WRITER_MODE);
                EsWriterMode esWriterMode = EsWriterMode.fromString(mode);
                if (esWriterMode == null) {
                    throw new ConfigurationException(String.format("writerMode doesn't recognize : %s", mode));
                }
                eventMap.put(eventType, new EventProcessor(eventType, configuration, esWriterMode));
            }
        }
    }

    @Override
    public List<ChannelTask> batchWrite(List<ChannelTask> channelTasks) {

        BulkRequest bulkRequest = new BulkRequest();
        Map<String, ChannelTask> idTaskMap = new HashMap<>(channelTasks.size());
        for (ChannelTask channelTask : channelTasks) {
            EventType eventType = channelTask.getEventType();
            EventProcessor eventProcessor = eventMap.get(eventType);
            if (eventProcessor != null) {
                Record record = channelTask.getRecord();
                String id = getId(record);
                idTaskMap.put(id, channelTask);
                eventProcessor.bulk(bulkRequest, esClient, record, id);
            }
        }
        List<ChannelTask> failureChannelTasks = new ArrayList<>();
        try {
            BulkResponse response = esClient.bulk(bulkRequest);
            for (BulkItemResponse bulkItemResponse : response) {
                if (bulkItemResponse.isFailed()) {
                    bulkItemResponse.getId();
                    failureChannelTasks.add(idTaskMap.get(bulkItemResponse.getId()));
                    LOGGER.error("index fail!", bulkItemResponse.getFailure().getCause());
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return failureChannelTasks;
    }

    /*@Override
    public List<ChannelTask> batchWrite(List<ChannelTask> tasks) {
        BulkRequestBuilder bulkRequest = esClient.prepareBulk();
        Map<String, ChannelTask> idTaskMap = new HashMap<>(tasks.size());
        for (ChannelTask task : tasks) {
            Record record = task.getRecord();
            Map<String, Object> json = toMap(record);
            String id = getId(record);
            idTaskMap.put(id, task);
            if (update) {
                bulkRequest.add(esClient.prepareUpdate(index, type, id).setDoc(json));
            } else {
                bulkRequest.add(esClient.prepareIndex(index, type, id).setSource(json));
            }
        }
        List<ChannelTask> failureTasks = new ArrayList<>();
        BulkResponse response = bulkRequest.execute().actionGet();
        for (BulkItemResponse bulkItemResponse : response) {
            if (bulkItemResponse.isFailed()) {
                bulkItemResponse.getId();
                failureTasks.add(idTaskMap.get(bulkItemResponse.getId()));
                LOGGER.error("index fail!", bulkItemResponse.getFailure().getCause());
            }
        }
        return failureTasks;
    }*/

    @Override
    public void write(ChannelTask channelTask) {
        EventType eventType = channelTask.getEventType();
        EventProcessor eventProcessor = eventMap.get(eventType);
        Record record = channelTask.getRecord();
        String id = getId(record);
        if (eventProcessor != null) {
            try {
                eventProcessor.process(esClient, record, id);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void destroy() {
        try {
            TransportClientFactory.close();
        } catch (IOException e) {
            LOGGER.error("esClient close fail!", e);
        }
    }

    private String getId(Record record) {
        String id;
        if (idColumn != null) {
            Column column = record.getColumn(idColumn);
            Objects.requireNonNull(column, String.format("id column : %s can not find", idColumn));
            id = column.getValue().toString();
        } else {
            Set<Column> columnSet = record.getPrimaryKeys();
            if (columnSet.size() != 1) {
                throw new RuntimeException("联合主键，必须指定id字段!");
            }
            id = columnSet.iterator().next().getValue().toString();
        }
        return id;
    }

    private class EventProcessor extends Configurable {

        private final EventType eventType;

        private final Configuration configuration;

        private final EsWriterMode esWriterMode;

        private List<String> columns;

        private static final String COLUMNS = "columns";

        private boolean allColumns;

        public EventProcessor(EventType eventType, Configuration configuration, EsWriterMode esWriterMode) throws ConfigurationException {
            this.eventType = eventType;
            this.configuration = configuration;
            this.esWriterMode = esWriterMode;
            this.setConfiguration(configuration);
            if (esWriterMode.needDoc()) {
                this.columns = configuration.getNecessaryList(COLUMNS);
                allColumns = columns.size() == 1 && columns.get(0).equals("*");
            }
        }

        public void bulk(BulkRequest bulkRequest, RestHighLevelClient esClient, Record record, String id) {
            Map<String, Object> json = null;
            if (esWriterMode.needDoc()) {
                json = toMap(record);
            }
            esWriterMode.bulk(bulkRequest, esClient, index, type, id, json);
        }

        public void process(RestHighLevelClient esClient, Record record, String id) throws IOException {
            Map<String, Object> json = null;
            if (esWriterMode.needDoc()) {
                json = toMap(record);
            }
            esWriterMode.process(esClient, index, type, id, json);
        }

        private Map<String, Object> toMap(Record record) {
            Map<String, Object> map = new HashMap<>(record.getColumns().size());
            if (allColumns) {
                for (Column value : record.getColumns().values()) {
                    map.put(value.getName(), value.getValue());
                }
            } else {
                for (String column : columns) {
                    Column c = record.getColumn(column);
                    map.put(column, c == null ? null : c.getValue());
                }
            }
            return map;
        }

    }
}
