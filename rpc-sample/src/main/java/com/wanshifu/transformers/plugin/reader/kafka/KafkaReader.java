package com.wanshifu.transformers.plugin.reader.kafka;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.parser.Feature;
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
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.*;

public class KafkaReader extends Configurable implements Reader {


    private static final String SERVERS = "servers";
    private static final String GROUP_ID = "groupId";
    private static final String TOPIC = "topic";
    private static final String DATABASE_TABLE = "dataBaseTable";

    private String topic;
    private String servers;
    private String groupId;
    private Set<String> dataBaseTableSet;
    private String dataBaseTable;

    private KafkaConsumer<String, String> consumer;

    private volatile boolean flag;

    private static final Object lock = new Object();


    @Override
    public void init() throws InitException {
        Configuration parameter = getConfiguration().getConfiguration(CoreConstants.PARAMETER);
        servers = parameter.getNecessaryValue(SERVERS);
        groupId = parameter.getNecessaryValue(GROUP_ID);
        topic = parameter.getNecessaryValue(TOPIC);
        dataBaseTable = parameter.getNecessaryValue(DATABASE_TABLE);
        //首先根据,拆分多个
        //然后把相应的库和表放到set里面
        dataBaseTableSet = new HashSet<>(Arrays.asList(dataBaseTable.split(",")));

        Properties props = new Properties();
        props.put("bootstrap.servers", servers);
        props.put("group.id", groupId);
        props.put("enable.auto.commit", "false");
        props.put("auto.offset.reset", "latest");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        consumer = new KafkaConsumer<>(props);
        flag = true;
    }

    @Override
    public void startReader(Transport transport) {
        consumer.subscribe(Arrays.asList(topic.split(",")));
        while (flag) {
            synchronized (lock) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                consumer.commitAsync();
                Date receiveDate = new Date();
                for (ConsumerRecord<String, String> consumerRecord : records) {
                    String jsonStr = consumerRecord.value();
                    JSONObject object = JSONObject.parseObject(jsonStr, Feature.AllowUnQuotedFieldNames);
                    String dbStr = object.get("database").toString();
                    String tableStr = object.get("table").toString();
                    String dataBaseTableStr = dbStr + "." + tableStr;
                    //过滤未订阅的相关数据库和表
                    if (!dataBaseTableSet.contains(dataBaseTableStr)) {
                        continue;
                    }
                    String typeStr = object.get("type").toString().toLowerCase();
                    EventType eventType = EventType.fromString(typeStr);
                    if (eventType == null) {
                        continue; //跳过不支持的事件
                    }

                    JSONArray dataArrays = object.getJSONArray("data");
                    //拿改变 data 的值 构建每一组数据
                    List<Map<String, Object>> recordDataList = getRecordDataMap(dataArrays);


                    //拿mysqlType
                    Map<String, Object> recordTypeMap = getRecordTypeMap(object);
                    Map<String, Object> sqlTypeMap = getSqlTypeMap(object);
                    Set<String> pkNameSet = getPkNameSet(object);
                    for (Map<String, Object> recordDataMap : recordDataList) {
                        Iterator fieldNameIterator = recordTypeMap.keySet().iterator();
                        Record record = new Record();
                        while (fieldNameIterator.hasNext()) {
                            String columnName = (String) fieldNameIterator.next();//每一行数据的字段名
                            Object value;//对应字段的值
                            if (null == recordDataMap.get(columnName)) {
                                value = null;
                            } else {
                                value = recordDataMap.get(columnName);
                            }
                            String type = recordTypeMap.get(columnName).toString();
                            int sqlType = (int) sqlTypeMap.get(columnName);
                            boolean isPrimaryKey = pkNameSet.contains(columnName);
                            Column column = ColumnFactory.createFromStringByMysqlColumnMeta(new MysqlColumnMetaData(columnName, sqlType, type), value == null ? null : value.toString(), isPrimaryKey);
                            record.addColumn(column);
                        }

                        record.setTmpTime(receiveDate);
                        Task channelTask = new Task("kafka", dbStr, tableStr, eventType, record);
                        transport.send(channelTask);
                    }
                }
            }
        }
    }


    @Override
    public void destroy() {
        flag = false;
        synchronized (lock) {
            consumer.unsubscribe();
            consumer.close();
        }
    }


    private List<Map<String, Object>> getRecordDataMap(JSONArray dataArrays) {
        List<Map<String, Object>> valueList = new ArrayList<>();
        dataArrays.forEach(dataArray -> {
            Map<String, Object> recordDataMap = new HashMap<>();
            JSONObject dataObject = (JSONObject) dataArray;
            Set<Map.Entry<String, Object>> set = dataObject.entrySet();
            for (Map.Entry<String, Object> entryMap : set) {
                recordDataMap.put(entryMap.getKey(), entryMap.getValue());
            }
            valueList.add(recordDataMap);
        });
        return valueList;
    }

    private Map<String, Object> getRecordTypeMap(JSONObject object) {
        return object.getJSONObject("mysqlType");
    }

    private Map<String, Object> getSqlTypeMap(JSONObject object) {
        return object.getJSONObject("sqlType");
    }


    private Set<String> getPkNameSet(JSONObject object) {
        JSONArray pkNamesArray = object.getJSONArray("pkNames");
        Set pkNameSet = new HashSet(pkNamesArray);
        return pkNameSet;
    }


}
