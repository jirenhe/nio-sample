package com.wanshifu.transformers.plugin.reader.mysql;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.UnexpectedException;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.bean.Task;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.*;
import com.wanshifu.transformers.core.job.transport.Transport;
import com.wanshifu.transformers.plugin.reader.Reader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MySqlReader extends Configurable implements Reader {

    private static final String URL = "url";

    private static final String USERNAME = "username";

    private static final String PASSWORD = "password";

    private static final String SQL = "sql";

    private static final String TABLE = "table";

    private static final String DATA_BASE = "dataBase";

    private static final String COLUMNS = "columns";

    private static final String PRIMARY_KEYS = "primaryKeys";

    private static final String SPLIT = "split";

    private static final String SPLIT_THREAD_COUNT = "split.threadCount";

    private static final String SPLIT_PK = "split.splitPk";

    private static final String SPLIT_RANGE = "split.range";

    private static final String SPLIT_RANGE_START = "split.range.start";

    private static final String SPLIT_RANGE_END = "split.range.end";

    private String sql;

    private ThreadPoolExecutor threadPoolExecutor;

    private CountDownLatch countDownLatch;

    private int threadCount = 1;

    private long start;

    private long end;

    private String url;

    private String userName;

    private String password;

    private String db;

    private String table;

    private String splitPk;

    private Set<String> primaryKeySet;

    private volatile boolean shutDownFlag = false;

    private boolean split;

    private final Logger LOGGER = LoggerFactory.getLogger(MySqlReader.class);

    @Override
    public void init() throws InitException {
        Configuration parameter = getConfiguration().getConfiguration(CoreConstants.PARAMETER);
        url = appendUrlConfig(parameter.getNecessaryValue(URL));
        userName = parameter.getNecessaryValue(USERNAME);
        password = parameter.getNecessaryValue(PASSWORD);
        db = parameter.getNecessaryValue(DATA_BASE);
        split = parameter.getConfiguration(SPLIT) != null;
        boolean testConn = DBUtil.testConnWithoutRetry(DataBaseType.MySql, url, userName, password, Collections.singletonList("select 0 from dual"));
        if (!testConn) {
            throw new InitException("数据库连接异常！请检查配置!");
        }
        String primaryKeys = parameter.getNecessaryValue(PRIMARY_KEYS);
        primaryKeySet = new HashSet<>(Arrays.asList(primaryKeys.split(",")));
        table = parameter.getNecessaryValue(TABLE);
        String sourceSql = parameter.getString(SQL);
        if (StringUtils.isEmpty(sourceSql)) {
            List<String> columns = parameter.getList(COLUMNS);
            if (StringUtils.isEmpty(table) || columns.size() == 0) {
                throw new ConfigurationException("未配置自定义sql时必须配置表名和列");
            }
            this.sql = createSql(table, columns);
        } else {
            checkSql(sourceSql);
            this.sql = sourceSql;
        }
        if (split) {
            splitPk = parameter.getNecessaryValue(SPLIT_PK);
            threadCount = parameter.getInt(SPLIT_THREAD_COUNT, 1);
            calculateRange(this.sql, splitPk, parameter);
            this.sql = appendSplitCondition(sql);
        }
        threadPoolExecutor = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadCount, new NamedThreadFactory("MySqlReader-runner-"));
        countDownLatch = new CountDownLatch(threadCount);
        LOGGER.info("init successful! execute sql is {}", this.sql);
    }

    @Override
    public void startReader(Transport transport) {
        if (!split) {
            threadPoolExecutor.execute(new Runner(-1, -1, transport));
        } else {
            long totalCount = end - start + 1;
            long piecesCount = totalCount / threadCount;
            for (int i = 0; i < threadCount; i++) {
                long startIndex = start + piecesCount * i;
                long endIndex = startIndex + piecesCount - 1;
                if (i == threadCount - 1) {
                    endIndex = end;
                }
                threadPoolExecutor.execute(new Runner(startIndex, endIndex, transport));
            }
        }
        threadPoolExecutor.shutdown();
        try {
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new UnexpectedException(e);
        }
        if (!shutDownFlag) {
            transport.done();
        }
    }

    @Override
    public void destroy() {
        shutDownFlag = true;
    }

    private void calculateRange(String sourceSql, String splitPk, Configuration parameter) throws InitException {
        String max;
        String min;
        if (parameter.getConfiguration(SPLIT_RANGE) != null) {
            String endTmp = parameter.getString(SPLIT_RANGE_END);
            String startTmp = parameter.getString(SPLIT_RANGE_START);
            boolean minFigureOut = false, maxFigureOut = false;
            if ("max".equalsIgnoreCase(endTmp)) {
                max = "MAX(" + splitPk + ")";
            } else {
                try {
                    end = Long.valueOf(endTmp);
                } catch (NumberFormatException e) {
                    throw new ConfigurationException(String.format("parameter : %s config error! it is not a number!", SPLIT_RANGE_END));
                }
                max = endTmp;
                maxFigureOut = true;
            }
            if ("min".equalsIgnoreCase(startTmp)) {
                min = "MIN(" + splitPk + ")";
            } else {
                try {
                    start = Long.valueOf(startTmp);
                } catch (NumberFormatException e) {
                    throw new ConfigurationException(String.format("parameter : %s config error! it is not a number!", SPLIT_RANGE_START));
                }
                min = startTmp;
                minFigureOut = true;
            }
            if (maxFigureOut && minFigureOut) return;
        } else {
            max = "MAX(" + splitPk + ")";
            min = "MIN(" + splitPk + ")";
        }
        ResultSet resultSet = null;
        try (Connection connection = DBUtil.getConnection(DataBaseType.MySql, url, userName, password)) {
            String rangeSql = genRangeSql(sourceSql, min, max);
            resultSet = DBUtil.query(connection, rangeSql);
            resultSet.next();
            start = resultSet.getLong(1);
            end = resultSet.getLong(2);
        } catch (SQLException e) {
            throw new InitException(e);
        } finally {
            if (resultSet != null) {
                try {
                    resultSet.close();
                } catch (SQLException ignore) {
                }
            }
        }
    }

    private String genRangeSql(String sourceSql, String min, String max) {
        return Pattern.compile("select .* from").matcher(
                sourceSql).replaceFirst(Matcher.quoteReplacement(String.format("select %s,%s from", min, max)));
    }

    private String appendSplitCondition(String sql) {
        if (sql.contains("where")) {
            return sql + String.format(" and(%s >= ? and %s <= ?)", splitPk, splitPk);
        } else {
            return sql + String.format(" where 1=1 and(%s >= ? and %s <= ?)", splitPk, splitPk);
        }
    }

    private String createSql(String table, List<String> columns) {
        StringBuilder sqlBuilder = new StringBuilder("select ");
        if (columns.size() == 1 && "*".equals(columns.get(0))) {
            sqlBuilder.append("*");
        } else {
            for (int i = 0; i < columns.size() - 1; i++) {
                sqlBuilder.append(columns.get(i)).append(",");
            }
            sqlBuilder.append(columns.get(columns.size() - 1));
        }
        sqlBuilder.append(" from ").append(table);
        sqlBuilder.append(" where 1=1");
        return sqlBuilder.toString();
    }

    private void checkSql(String sql) {

    }

    private String appendUrlConfig(String url) {
        if (url.contains("?")) {
            return url + "&tinyInt1isBit=false";
        } else {
            return url + "?tinyInt1isBit=false";
        }
    }

    private class Runner implements Runnable {

        private final long startIndex;

        private final long endIndex;

        private final Transport transport;

        public Runner(long startIndex, long endIndex, Transport transport) {
            this.startIndex = startIndex;
            this.endIndex = endIndex;
            this.transport = transport;
        }

        @Override
        public void run() {
            Connection connection = null;
            ResultSet rs = null;
            long count = 0;
            try {
                connection = DBUtil.getConnection(DataBaseType.MySql,
                        url,
                        userName,
                        password
                );
                if (startIndex > -1 && endIndex > -1) {
                    rs = DBUtil.query(connection, sql, Integer.MIN_VALUE, Arrays.asList(startIndex, endIndex));
                } else {
                    rs = DBUtil.query(connection, sql, Integer.MIN_VALUE);
                }
                ResultSetMetaData metaData = rs.getMetaData();
                while (rs.next() && !shutDownFlag) {
                    this.transportOneRecord(transport, rs, metaData);
                    count++;
                }
            } catch (Throwable e) {
                LOGGER.error("read fail!", e);
            } finally {
                DBUtil.closeDBResources(rs, null, connection);
                countDownLatch.countDown();
                LOGGER.info("mysql read : {} finish startIndex : {} endIndex : {}, total readCount : {}",
                        Thread.currentThread().getName(),
                        startIndex,
                        endIndex,
                        count
                );
            }
        }

        private void transportOneRecord(Transport transport, ResultSet rs, ResultSetMetaData metaData) throws SQLException {
            Record record = new Record();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                String columnName = metaData.getColumnName(i);
                String sourceType = metaData.getColumnTypeName(i);
                int sqlType = metaData.getColumnType(i);
                Column column = ColumnFactory.createFromResultSetByMysqlColumnMeta(new MysqlColumnMetaData(columnName, sqlType, sourceType), rs, primaryKeySet.contains(columnName));
                record.addColumn(column);
            }
            Task task = new Task("mysql", db, table, EventType.INSERT, record);
            transport.send(task);
        }
    }
}
