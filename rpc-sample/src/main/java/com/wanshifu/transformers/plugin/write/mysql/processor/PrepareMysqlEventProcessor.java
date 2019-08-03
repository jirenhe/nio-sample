package com.wanshifu.transformers.plugin.write.mysql.processor;

import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.plugin.write.mysql.MysqlWriteMode;
import com.wanshifu.transformers.plugin.write.mysql.WriterDigest;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class PrepareMysqlEventProcessor extends AbstractMysqlEventProcessor implements MysqlEventProcessor {

    private PrepareSqlDetail prepareSqlDetail;

    public PrepareMysqlEventProcessor(EventType eventType, Configuration configuration, WriterDigest writerDigest) throws ConfigurationException {
        super(eventType, configuration, writerDigest);
        this.prepareSqlDetail = createSqlByWriteMode(mysqlWriteMode);
    }

    @Override
    public StatementHolder newStatementHolder(Connection connection) throws SQLException {
        return new PrepareStatementHolder(connection);
    }

    @Override
    public String info() {
        return String.format("eventType : %s sql : %s", eventType.toString(), prepareSqlDetail.getPreparedSql());
    }


    private PrepareSqlDetail createSqlByWriteMode(MysqlWriteMode mode) throws ConfigurationException {
        try {
            return mode.resolveSqlDetail(writerDigest.getTable(), columns, columnsMetaData, conditionColumns);
        } catch (Exception e) {
            throw new ConfigurationException(e);
        }
    }

    private class PrepareStatementHolder implements StatementHolder {

        private PreparedStatement preparedStatement;

        private PrepareStatementHolder(Connection connection) throws SQLException {
            preparedStatement = connection.prepareStatement(prepareSqlDetail.getPreparedSql());
        }

        @Override
        public void execute(Record record) throws SQLException {
            this.fillStatement(record);
            this.preparedStatement.execute();
        }

        @Override
        public void executeBatch() throws SQLException {
            this.preparedStatement.executeBatch();
        }

        @Override
        public void addBath(Record record) throws SQLException {
            this.fillStatement(record);
            this.preparedStatement.addBatch();
        }

        @Override
        public void cleanUp() throws SQLException {
            try {
                this.preparedStatement.clearParameters();
            } finally {
                this.preparedStatement.close();
            }
            this.preparedStatement = null; //help GC
        }

        private void fillStatement(Record record) throws SQLException {
            prepareSqlDetail.fillStatement(preparedStatement, record);
        }

    }

}
