package com.wanshifu.transformers.plugin.reader.cannal;

import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.CanalEntry.RowChange;
import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.bean.Task;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.ColumnFactory;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.MysqlColumnMetaData;
import com.wanshifu.transformers.core.job.transport.Transport;
import com.wanshifu.transformers.plugin.reader.Reader;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class CanalReader extends Configurable implements Reader {

    private static final String HOST = "host";

    private static final String PORT = "port";

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    private static final String DESTINATION = "destination";

    private static final String BATCH_SEIZE = "batchSize";

    private static final int DEFAULT_BATCH_SEIZE = 256;

    private static final String DATA_BASE_TABLE = "dataBaseTable";

    private CanalConnectorCollection connectorCollection;

    private final Logger LOGGER = LoggerFactory.getLogger(CanalReader.class);

    @Override
    public void init() throws InitException {
        List<Configuration> parameters = getConfiguration().getListConfiguration((CoreConstants.PARAMETER));
        connectorCollection = new CanalConnectorCollection(parameters.size());
        for (Configuration parameter : parameters) {
            String host = parameter.getNecessaryValue(HOST);
            int port = parameter.getNecessaryInt(PORT);
            String userName = parameter.getNecessaryValue(USERNAME);
            String password = parameter.getNecessaryValue(PASSWORD);
            String destination = parameter.getNecessaryValue(DESTINATION);
            String dataBaseTable = parameter.getNecessaryValue(DATA_BASE_TABLE); // .*代表database，..*代表table
            int batchSize = parameter.getInt(BATCH_SEIZE, DEFAULT_BATCH_SEIZE);
            CanalMetaData canalMetaData = CanalMetaData.builder()
                    .host(host)
                    .port(port)
                    .userName(userName)
                    .password(password)
                    .destination(destination)
                    .dataBaseTable(dataBaseTable)
                    .batchSize(batchSize)
                    .build();
            connectorCollection.add(canalMetaData);
        }
        LOGGER.info("canal reader init successful!");
    }


    @Override
    public void startReader(Transport transport) {

        try {
            connectorCollection.connect();
//                connectorCollection.subscribe(db + "\\." + table);// .*代表database，..*代表table
            connectorCollection.subscribe();
            connectorCollection.rollback();//
            connectorCollection.startFetch((entries, canalMetaData) -> transportEntryList(entries, transport, canalMetaData.getDestination()));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void transportEntryList(List<CanalEntry.Entry> entryList, Transport transport, String destination) {
        Date receiveDate = new Date();
        for (CanalEntry.Entry entry : entryList) {
            String dataBaseStr = entry.getHeader().getSchemaName();
            String tableNameStr = entry.getHeader().getTableName();
            RowChange rowChange;
            try {
                rowChange = RowChange.parseFrom(entry.getStoreValue());
            } catch (Exception e) {
                throw new RuntimeException(
                        "ERROR ## parser of eromanga-event has an error,data:"
                                + entry.toString(), e);
            }
            CanalEntry.EventType canalEventType = rowChange.getEventType();
            String typeStr = canalEventType.name().toLowerCase();
            EventType eventType = EventType.fromString(typeStr);
            if (eventType == null) {
                continue; //跳过不支持的事件
            }

            for (CanalEntry.RowData rowData : rowChange.getRowDatasList()) {
                List<CanalEntry.Column> rowDataList;
                if (canalEventType == CanalEntry.EventType.DELETE) {
                    rowDataList = rowData.getBeforeColumnsList();
                } else {
                    rowDataList = rowData.getAfterColumnsList();
                }

                if (CollectionUtils.isEmpty(rowDataList)) {
                    continue;
                }

                transport.send(toTask(rowDataList, eventType, receiveDate, dataBaseStr, tableNameStr, destination));
            }
        }
    }

    private Task toTask(List<CanalEntry.Column> rowDataList, EventType eventType, Date receiveDate, String dataBaseStr, String tableNameStr, String destination) {
        Record record = new Record();
        rowDataList.forEach(canalColumn -> {
            String columnName = canalColumn.getName();//每一行数据的字段名
            String value = canalColumn.hasValue() ? canalColumn.getValue() : null;//对应字段的值
            boolean isPrimaryKey = canalColumn.getIsKey();
            String sourceType = canalColumn.getMysqlType();
            int sqlType = canalColumn.getSqlType();
            Column column = ColumnFactory.createFromStringByMysqlColumnMeta(new MysqlColumnMetaData(columnName, sqlType, sourceType), value, isPrimaryKey);
            record.addColumn(column);
        });
        record.setTmpTime(receiveDate);
        return new Task("canal-" + destination, dataBaseStr, tableNameStr, eventType, record);
    }

    @Override
    public void destroy() {
        connectorCollection.disconnect();
    }
}

