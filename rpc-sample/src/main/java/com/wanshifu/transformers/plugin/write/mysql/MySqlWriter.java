package com.wanshifu.transformers.plugin.write.mysql;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.common.utils.DBUtil;
import com.wanshifu.transformers.common.utils.DataBaseType;
import com.wanshifu.transformers.plugin.write.WriteException;
import com.wanshifu.transformers.plugin.write.Writer;
import com.wanshifu.transformers.plugin.write.mysql.processor.DynamicSqlMysqlEventProcessor;
import com.wanshifu.transformers.plugin.write.mysql.processor.MysqlEventProcessor;
import com.wanshifu.transformers.plugin.write.mysql.processor.PrepareMysqlEventProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

public class MySqlWriter extends Configurable implements Writer {

    private static final String URL = "url";

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    private static final String TABLE = "table";

    private static final String EVENT_PROCESS = "eventProcess";

    private static final String STATIC_SQL = "staticSql";

    private WriterDigest writerDigest;

    private boolean staticSql;

    private Connection connection;

    private final Map<EventType, MysqlEventProcessor> eventMap = new HashMap<>();

    private final Logger LOGGER = LoggerFactory.getLogger(MySqlWriter.class);

    @Override
    public void init() throws InitException {
        Configuration parameter = getConfiguration().getNecessaryConfiguration(CoreConstants.PARAMETER);
        String url = parameter.getNecessaryValue(URL);
        String userName = parameter.getNecessaryValue(USERNAME);
        String password = parameter.getNecessaryValue(PASSWORD);
        String table = parameter.getNecessaryValue(TABLE);
        staticSql = parameter.getNecessaryBool(STATIC_SQL);
        url = appendUrlConfig(url);
        boolean testConn = DBUtil.testConnWithoutRetry(DataBaseType.MySql, url, userName, password, Collections.singletonList("select 0 from dual"));
        if (!testConn) {
            throw new InitException("数据库连接异常！请检查配置!");
        }
        this.writerDigest = new WriterDigest(url, userName, password, table);
        Configuration eventProcess = parameter.getNecessaryConfiguration(EVENT_PROCESS);
        initEventSqlMap(eventProcess);
        this.connection = DBUtil.getConnection(DataBaseType.MySql, url, userName, password);
        StringBuilder stringBuilder = new StringBuilder();
        for (Map.Entry<EventType, MysqlEventProcessor> eventTypeMysqlEventProcessorEntry : eventMap.entrySet()) {
            stringBuilder.append("\r\n").append(eventTypeMysqlEventProcessorEntry.getValue().info());
        }
        LOGGER.info("init successful! execute sql is {}", stringBuilder.toString());
    }

    private void initEventSqlMap(Configuration eventProcess) throws ConfigurationException {
        for (EventType eventType : EventType.values()) {
            Configuration configuration = eventProcess.getConfiguration(eventType.toString());
            if (configuration != null) {
                if (staticSql) {
                    eventMap.put(eventType, new PrepareMysqlEventProcessor(eventType, configuration, writerDigest));
                } else {
                    eventMap.put(eventType, new DynamicSqlMysqlEventProcessor(eventType, configuration, writerDigest));
                }
            }
        }
    }

    @Override
    public List<ChannelTask> batchWrite(List<ChannelTask> channelTasks) throws WriteException {
        int size = channelTasks.size();
        long time = System.currentTimeMillis();
        List<ChannelTask> failureChannelTasks = new ArrayList<>();
        Map<EventType, MysqlEventProcessor.StatementHolder> holderMap = new HashMap<>();
        try {
            connection.setAutoCommit(false);
            for (ChannelTask channelTask : channelTasks) {
                MysqlEventProcessor.StatementHolder statementHolder;
                EventType eventType = channelTask.getEventType();
                Record record = channelTask.getRecord();
                if ((statementHolder = holderMap.get(eventType)) == null) {
                    MysqlEventProcessor mysqlEventProcessor = eventMap.get(eventType);
                    if (mysqlEventProcessor != null) {
                        statementHolder = mysqlEventProcessor.newStatementHolder(connection);
                        holderMap.put(eventType, statementHolder);
                    }
                }
                if (statementHolder != null) {
                    statementHolder.addBath(record);
                }
            }
            for (MysqlEventProcessor.StatementHolder statementHolder : holderMap.values()) {
                long time1 = System.currentTimeMillis();
                statementHolder.executeBatch();
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("execute batch size : {} take time : {}", size, System.currentTimeMillis() - time1);
                }
            }
            connection.commit();
        } catch (SQLException e) {
            LOGGER.warn("回滚此次写入, 采用每次写入一行方式提交.", e);
            try {
                connection.rollback();
            } catch (SQLException e1) {
                LOGGER.warn("回滚失败. 因为:" + e.getMessage());
            }
            for (ChannelTask channelTask : channelTasks) {
                try {
                    write(channelTask);
                } catch (Exception e1) {
                    failureChannelTasks.add(channelTask);
                }
            }
        } catch (Exception e) {
            throw new WriteException(e);
        } finally {
            for (MysqlEventProcessor.StatementHolder statementHolder : holderMap.values()) {
                try {
                    statementHolder.cleanUp();
                } catch (SQLException e) {
                    LOGGER.error("clean up fail!", e);
                }
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("batch writer task buffer size : {} take time : {}", size, System.currentTimeMillis() - time);
            }
        }
        return failureChannelTasks;
    }

    @Override
    public void write(ChannelTask channelTask) throws WriteException {
        EventType eventType = channelTask.getEventType();
        MysqlEventProcessor mysqlEventProcessor = eventMap.get(eventType);
        if (mysqlEventProcessor != null) {
            MysqlEventProcessor.StatementHolder statementHolder = null;
            try {
                connection.setAutoCommit(true);
                statementHolder = mysqlEventProcessor.newStatementHolder(connection);
                Record record = channelTask.getRecord();
                try {
                    statementHolder.execute(record);
                } catch (SQLException e) {
                    throw new WriteException(e);
                }
            } catch (Exception e) {
                throw new WriteException(e);
            } finally {
                if (statementHolder != null) {
                    try {
                        statementHolder.cleanUp();
                    } catch (SQLException e) {
                        LOGGER.error("clean up fail!", e);
                    }
                }
            }
        }
    }

    @Override
    public void destroy() {
        try {
            connection.close();
        } catch (SQLException ignore) {
        }
    }

    private String appendUrlConfig(String url) {
        if (url.contains("?")) {
            return url + "&yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
        } else {
            return url + "?yearIsDateType=false&zeroDateTimeBehavior=convertToNull&tinyInt1isBit=false&rewriteBatchedStatements=true";
        }
    }
}
