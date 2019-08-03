package com.wanshifu.transformers.plugin.write.mysql.processor;

import com.wanshifu.transformers.common.bean.Record;

import java.sql.Connection;
import java.sql.SQLException;

public interface MysqlEventProcessor {

    StatementHolder newStatementHolder(Connection connection) throws SQLException;

    String info();

    interface StatementHolder {

        void execute(Record record) throws SQLException;

        void executeBatch() throws SQLException;

        void addBath(Record record) throws SQLException;

        void cleanUp() throws SQLException;
    }
}
