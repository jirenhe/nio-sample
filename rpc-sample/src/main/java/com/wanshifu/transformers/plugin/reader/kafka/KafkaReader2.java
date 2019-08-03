package com.wanshifu.transformers.plugin.reader.kafka;

import com.alibaba.otter.canal.client.kafka.KafkaCanalConnector;
import com.alibaba.otter.canal.protocol.FlatMessage;
import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.utils.ColumnFactory;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.bean.Task;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.MysqlColumnMetaData;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.NamedThreadFactory;
import com.wanshifu.transformers.core.job.transport.Transport;
import com.wanshifu.transformers.plugin.reader.Reader;

import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class KafkaReader2 extends Configurable implements Reader {


    private static final String ZK_CONNECT = "zkConnect";

    private static final String GROUP_ID = "groupId";

    private static final String TOPIC = "topic";

    private static final String SERVERS = "servers";

    private static final String PRIMARY_KEYS = "primaryKeys";

    private static final String SPLIT_THREAD_COUNT = "split.threadCount";

    private static final String BATCH_SEIZE = "batchSize";

    private ThreadPoolExecutor threadPoolExecutor;

    private String topic;

    private String groupId;

    private int threadCount = 1;

    private Integer batchSize;

    private volatile boolean flag;

    private static final Object lock = new Object();

    private String servers;

    private KafkaCanalConnector connector;

    private Set<String> primaryKeySet;


    @Override
    public void init() throws InitException {
        Configuration parameter = getConfiguration().getConfiguration(CoreConstants.PARAMETER);
        servers = parameter.getNecessaryValue(SERVERS);
        groupId = parameter.getNecessaryValue(GROUP_ID);
        batchSize = parameter.getNecessaryInt(BATCH_SEIZE);
        topic = parameter.getNecessaryValue(TOPIC);
        threadCount = parameter.getInt(SPLIT_THREAD_COUNT, 1);

        String primaryKeys = parameter.getNecessaryValue(PRIMARY_KEYS);
        primaryKeySet = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("KafkaReader-runner-"));
        connector = new KafkaCanalConnector(servers, topic, null, groupId, batchSize, true);
        flag = true;
    }

    @Override
    public void startReader(Transport transport) {
        while (flag) {
            synchronized (lock) {
                connector.connect();
                connector.subscribe();
                List<FlatMessage> messageList = connector.getFlatList(100L, TimeUnit.MILLISECONDS);
                Date receiveDate = new Date();
                connector.ack();
                if (null != messageList) {
                    for (FlatMessage flatMessage : messageList) {
                        String typeStr = flatMessage.getType().toLowerCase();
                        EventType eventType = EventType.fromString(typeStr);
                        if (eventType == null) {
                            continue; //跳过不支持的事件
                        }

                        //数据
                        List<java.util.Map<String, String>> dataList = flatMessage.getData();
                        //对应的mysqlType
                        Map<String, String> mysqlTypeMap = flatMessage.getMysqlType();
                        Map<String, Integer> sqlTypeMap = flatMessage.getSqlType();
                        for (Map<String, String> recordMap : dataList) {//dataList可能是多个数组多条记录,每个map就是一行记录
                            Record record = new Record();
                            //取出对应的key
                            Set<String> fieldNameSet = recordMap.keySet();
                            for (Map.Entry<String, String> stringStringEntry : recordMap.entrySet()) {
                                String fieldName = stringStringEntry.getKey();
                                //fieldName就是字段名
                                //mysqlTypeMap 对应 fieldName的就是mysqlType
                                String sourceType = mysqlTypeMap.get(fieldName);
                                int sqlType = sqlTypeMap.get(fieldName);

                                //recordMap 对应fieldName的就是改变的数据value
                                String value = recordMap.get(fieldName);
                                Column column = ColumnFactory.createFromStringByMysqlColumnMeta(new MysqlColumnMetaData(fieldName, sqlType, sourceType), value, primaryKeySet.contains(fieldName));
                                record.addColumn(column);
                            }
                            record.setTmpTime(receiveDate);
                            transport.send(new Task("kafka", flatMessage.getDatabase(), flatMessage.getTable(), eventType, record));
                        }
                    }
                }
            }
        }
    }


    @Override
    public void destroy() {
        flag = false;
        synchronized (lock) {
            try {
                connector.unsubscribe();
                connector.disconnect();
                threadPoolExecutor.shutdown();
                while (!threadPoolExecutor.awaitTermination(50, TimeUnit.MILLISECONDS)) {
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }


}
