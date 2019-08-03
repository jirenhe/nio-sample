package com.wanshifu.transformers.plugin.write.mysql.processor;

import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.plugin.write.mysql.WriterDigest;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

public class DynamicSqlMysqlEventProcessor extends AbstractMysqlEventProcessor implements MysqlEventProcessor {

    public DynamicSqlMysqlEventProcessor(EventType eventType, Configuration configuration, WriterDigest writerDigest) throws ConfigurationException {
        super(eventType, configuration, writerDigest);
    }

    @Override
    public StatementHolder newStatementHolder(Connection connection) throws SQLException {
        return new SampleStatementHolder(connection);
    }

    @Override
    public String info() {
        return String.format("eventType : %s sql ", eventType.toString());
    }

    private class SampleStatementHolder implements StatementHolder {

        private Statement statement;

        private SampleStatementHolder(Connection connection) throws SQLException {
            statement = connection.createStatement();
        }

        @Override
        public void execute(Record record) throws SQLException {
            this.statement.execute(toSql(writerDigest.getTable(), record));
        }

        @Override
        public void executeBatch() throws SQLException {
            this.statement.executeBatch();
        }

        @Override
        public void addBath(Record record) throws SQLException {
            this.statement.addBatch(toSql(writerDigest.getTable(), record));
        }

        @Override
        public void cleanUp() throws SQLException {
            this.statement.close();
            this.statement = null; //help GC
        }

        private String toSql(String table, Record record) {
            return mysqlWriteMode.createSql(table, record, columns, columnsMetaData, conditionColumns);
        }

    }
}
