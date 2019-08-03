package com.wanshifu.transformers.common.utils;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.wanshifu.transformers.common.UnexpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

public final class DBUtil {
    private static final Logger LOG = LoggerFactory.getLogger(DBUtil.class);

    public static final int SOCKET_TIMEOUT_INSECOND = 172800;

    public static final int TIMEOUT_SECONDS = 15;

    public static final String SESSION = "session";

    private static final ThreadLocal<ExecutorService> rsExecutors = ThreadLocal.withInitial(() -> Executors.newFixedThreadPool(1, new ThreadFactoryBuilder()
            .setNameFormat("rsExecutors-%d")
            .setDaemon(true)
            .build()));

    private DBUtil() {
    }

    /**
     * 检查slave的库中的数据是否已到凌晨00:00
     * 如果slave同步的数据还未到00:00返回false
     * 否则范围true
     *
     * @author ZiChi
     * @version 1.0 2014-12-01
     */
    private static boolean isSlaveBehind(Connection conn) {
        try {
            ResultSet rs = query(conn, "SHOW VARIABLES LIKE 'read_only'");
            if (DBUtil.asyncResultSetNext(rs)) {
                String readOnly = rs.getString("Value");
                if ("ON".equalsIgnoreCase(readOnly)) { //备库
                    ResultSet rs1 = query(conn, "SHOW SLAVE STATUS");
                    if (DBUtil.asyncResultSetNext(rs1)) {
                        String ioRunning = rs1.getString("Slave_IO_Running");
                        String sqlRunning = rs1.getString("Slave_SQL_Running");
                        long secondsBehindMaster = rs1.getLong("Seconds_Behind_Master");
                        if ("Yes".equalsIgnoreCase(ioRunning) && "Yes".equalsIgnoreCase(sqlRunning)) {
                            ResultSet rs2 = query(conn, "SELECT TIMESTAMPDIFF(SECOND, CURDATE(), NOW())");
                            DBUtil.asyncResultSetNext(rs2);
                            long secondsOfDay = rs2.getLong(1);
                            return secondsBehindMaster > secondsOfDay;
                        } else {
                            return true;
                        }
                    } else {
                        LOG.warn("SHOW SLAVE STATUS has no result");
                    }
                }
            } else {
                LOG.warn("SHOW VARIABLES like 'read_only' has no result");
            }
        } catch (Exception e) {
            LOG.warn("checkSlave failed, errorMessage:[{}].", e.getMessage());
        }
        return false;
    }

    /**
     * 检查表是否具有insert 权限
     * insert on *.* 或者 insert on database.* 时验证通过
     * 当insert on database.tableName时，确保tableList中的所有table有insert 权限，验证通过
     * 其它验证都不通过
     *
     * @author ZiChi
     * @version 1.0 2015-01-28
     */
    public static boolean hasInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) throws SQLException {
        /*准备参数*/

        String[] urls = jdbcURL.split("/");
        String dbName;
        if (urls.length != 0) {
            dbName = urls[3];
        } else {
            return false;
        }

        String dbPattern = "`" + dbName + "`.*";
        Collection<String> tableNames = new HashSet<>(tableList.size());
        tableNames.addAll(tableList);

        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        try {
            ResultSet rs = query(connection, "SHOW GRANTS FOR " + userName);
            while (DBUtil.asyncResultSetNext(rs)) {
                String grantRecord = rs.getString("Grants for " + userName + "@%");
                String[] params = grantRecord.split("\\`");
                if (params.length >= 3) {
                    String tableName = params[3];
                    if (params[0].contains("INSERT") && !tableName.equals("*"))
                        tableNames.remove(tableName);
                } else {
                    if (grantRecord.contains("INSERT") || grantRecord.contains("ALL PRIVILEGES")) {
                        if (grantRecord.contains("*.*"))
                            return true;
                        else if (grantRecord.contains(dbPattern)) {
                            return true;
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.warn("Check the database has the Insert Privilege failed, errorMessage:[{}]", e.getMessage());
        }
        return tableNames.isEmpty();
    }


    public static boolean checkInsertPrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) throws SQLException {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String insertTemplate = "insert into %s(select * from %s where 1 = 2)";

        boolean hasInsertPrivilege = true;
        Statement insertStmt;
        for (String tableName : tableList) {
            String checkInsertPrivilegeSql = String.format(insertTemplate, tableName, tableName);
            try {
                insertStmt = connection.createStatement();
                executeSqlWithoutResultSet(insertStmt, checkInsertPrivilegeSql);
            } catch (Exception e) {
                if (DataBaseType.Oracle.equals(dataBaseType)) {
                    if (e.getMessage() != null && e.getMessage().contains("insufficient privileges")) {
                        hasInsertPrivilege = false;
                        LOG.warn("User [" + userName + "] has no 'insert' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
                    }
                } else {
                    hasInsertPrivilege = false;
                    LOG.warn("User [" + userName + "] has no 'insert' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
                }
            }
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("connection close failed, " + e.getMessage());
        }
        return hasInsertPrivilege;
    }

    public static boolean checkDeletePrivilege(DataBaseType dataBaseType, String jdbcURL, String userName, String password, List<String> tableList) throws SQLException {
        Connection connection = connect(dataBaseType, jdbcURL, userName, password);
        String deleteTemplate = "delete from %s WHERE 1 = 2";

        boolean hasInsertPrivilege = true;
        Statement deleteStmt;
        for (String tableName : tableList) {
            String checkDeletePrivilegeSQL = String.format(deleteTemplate, tableName);
            try {
                deleteStmt = connection.createStatement();
                executeSqlWithoutResultSet(deleteStmt, checkDeletePrivilegeSQL);
            } catch (Exception e) {
                hasInsertPrivilege = false;
                LOG.warn("User [" + userName + "] has no 'delete' privilege on table[" + tableName + "], errorMessage:[{}]", e.getMessage());
            }
        }
        try {
            connection.close();
        } catch (SQLException e) {
            LOG.warn("connection close failed, " + e.getMessage());
        }
        return hasInsertPrivilege;
    }

    /**
     * Get direct JDBC connection
     * <p/>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p/>
     * NOTE: In DataX, we don't need connection pool in fact
     */
    public static Connection getConnection(final DataBaseType dataBaseType,
                                           final String jdbcUrl, final String username, final String password) {

        return getConnection(dataBaseType, jdbcUrl, username, password, String.valueOf(172800 * 1000));
    }

    /**
     * @param dataBaseType
     * @param jdbcUrl
     * @param username
     * @param password
     * @param socketTimeout 设置socketTimeout，单位ms，String类型
     * @return
     */
    public static Connection getConnection(final DataBaseType dataBaseType,
                                           final String jdbcUrl, final String username, final String password, final String socketTimeout) {

        try {
            return RetryUtil.executeWithRetry(() -> DBUtil.connect(dataBaseType, jdbcUrl, username,
                    password, socketTimeout), 9, 1000L, true);
        } catch (Exception e) {
            throw new UnexpectedException(String.format("数据库连接失败. 因为根据您配置的连接信息:%s获取数据库连接失败. 请检查您的配置并作出修改.", jdbcUrl), e);
        }
    }

    /**
     * Get direct JDBC connection
     * <p/>
     * if connecting failed, try to connect for MAX_TRY_TIMES times
     * <p/>
     * NOTE: In DataX, we don't need connection pool in fact
     */
    public static Connection getConnectionWithoutRetry(final DataBaseType dataBaseType,
                                                       final String jdbcUrl, final String username, final String password) throws SQLException {
        return getConnectionWithoutRetry(dataBaseType, jdbcUrl, username,
                password, String.valueOf(SOCKET_TIMEOUT_INSECOND * 1000));
    }

    public static Connection getConnectionWithoutRetry(final DataBaseType dataBaseType,
                                                       final String jdbcUrl, final String username, final String password, String socketTimeout) throws SQLException {
        return DBUtil.connect(dataBaseType, jdbcUrl, username,
                password, socketTimeout);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, String user, String pass) throws SQLException {
        return connect(dataBaseType, url, user, pass, String.valueOf(SOCKET_TIMEOUT_INSECOND * 1000));
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, String user, String pass, String socketTimeout) throws SQLException {

        Properties prop = new Properties();
        prop.put("user", user);
        prop.put("password", pass);

        if (dataBaseType == DataBaseType.Oracle) {
            //oracle.net.READ_TIMEOUT for jdbc versions < 10.1.0.5 oracle.jdbc.ReadTimeout for jdbc versions >=10.1.0.5
            // unit ms
            prop.put("oracle.jdbc.ReadTimeout", socketTimeout);
        }

        return connect(dataBaseType, url, prop);
    }

    private static synchronized Connection connect(DataBaseType dataBaseType,
                                                   String url, Properties prop) throws SQLException {
        try {
            Class.forName(dataBaseType.getDriverClassName());
        } catch (ClassNotFoundException e) {
            throw new UnexpectedException(e);
        }
        DriverManager.setLoginTimeout(TIMEOUT_SECONDS);
        return DriverManager.getConnection(url, prop);
    }

    public static ResultSet query(Connection conn, String sql)
            throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        //默认3600 seconds
        stmt.setQueryTimeout(SOCKET_TIMEOUT_INSECOND);
        return query(stmt);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, fetchSize, SOCKET_TIMEOUT_INSECOND, null);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn Database connection .
     * @param sql  sql statement to be executed
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, List<Object> params)
            throws SQLException {
        // 默认3600 s 的query Timeout
        return query(conn, sql, fetchSize, SOCKET_TIMEOUT_INSECOND, params);
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param conn         Database connection .
     * @param sql          sql statement to be executed
     * @param fetchSize
     * @param queryTimeout unit:second
     * @return
     * @throws SQLException
     */
    public static ResultSet query(Connection conn, String sql, int fetchSize, int queryTimeout, List<Object> params)
            throws SQLException {
        // make sure autocommit is off
        conn.setAutoCommit(false);
        PreparedStatement stmt;
        stmt = getStatement(conn, sql, params);
        stmt.setFetchSize(fetchSize);
        stmt.setQueryTimeout(queryTimeout);
        stmt.setFetchDirection(ResultSet.FETCH_REVERSE);
        return query(stmt);
    }

    private static PreparedStatement getStatement(Connection conn, String sql, List<Object> params) throws SQLException {
        PreparedStatement stmt = conn.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY);
        if(params != null){
            for (int i = 0; i < params.size(); i++) {
                stmt.setObject(i + 1, params.get(i));
            }
        }
        return stmt;
    }

    /**
     * a wrapped method to execute select-like sql statement .
     *
     * @param stmt {@link Statement}
     * @return a {@link ResultSet}
     * @throws SQLException if occurs SQLException.
     */
    public static ResultSet query(PreparedStatement stmt)
            throws SQLException {
        return stmt.executeQuery();
    }

    public static void executeSqlWithoutResultSet(Statement stmt, String sql)
            throws SQLException {
        stmt.execute(sql);
    }

    /**
     * Close {@link ResultSet}, {@link Statement} referenced by this
     * {@link ResultSet}
     *
     * @param rs {@link ResultSet} to be closed
     * @throws IllegalArgumentException
     */
    public static void closeResultSet(ResultSet rs) {
        try {
            if (null != rs) {
                Statement stmt = rs.getStatement();
                if (null != stmt) {
                    stmt.close();
                }
                rs.close();
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        }
    }

    public static void closeDBResources(ResultSet rs, Statement stmt,
                                        Connection conn) {
        if (null != rs) {
            try {
                rs.close();
            } catch (SQLException ignored) {
            }
        }

        if (null != stmt) {
            try {
                stmt.close();
            } catch (SQLException ignored) {
            }
        }

        if (null != conn) {
            try {
                conn.close();
            } catch (SQLException ignored) {
            }
        }
    }

    public static void closeDBResources(Statement stmt, Connection conn) {
        closeDBResources(null, stmt, conn);
    }

    public static List<String> getTableColumns(DataBaseType dataBaseType,
                                               String jdbcUrl, String user, String pass, String tableName) throws SQLException {
        Connection conn = getConnection(dataBaseType, jdbcUrl, user, pass);
        return getTableColumnsByConn(conn, tableName);
    }

    public static List<String> getTableColumnsByConn(Connection conn, String tableName) throws SQLException {
        List<String> columns = new ArrayList<>();
        Statement statement = null;
        ResultSet rs = null;
        String queryColumnSql;
        try {
            statement = conn.createStatement();
            queryColumnSql = String.format("select * from %s where 1=2",
                    tableName);
            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columns.add(rsMetaData.getColumnName(i + 1));
            }

        } finally {
            DBUtil.closeDBResources(rs, statement, conn);
        }

        return columns;
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static MysqlColumnMetaDataCollection getColumnMetaData(
            DataBaseType dataBaseType, String jdbcUrl, String user,
            String pass, String tableName, String column) throws SQLException {
        Connection conn = null;
        try {
            conn = getConnection(dataBaseType, jdbcUrl, user, pass);
            return getColumnMetaData(conn, tableName, column);
        } finally {
            DBUtil.closeDBResources(null, null, conn);
        }
    }

    /**
     * @return Left:ColumnName Middle:ColumnType Right:ColumnTypeName
     */
    public static MysqlColumnMetaDataCollection getColumnMetaData(
            Connection conn, String tableName, String column) throws SQLException {
        Statement statement = null;
        ResultSet rs = null;

        MysqlColumnMetaDataCollection columnMetaDataCollection = new MysqlColumnMetaDataCollection();

        try {
            statement = conn.createStatement();
            String queryColumnSql = "select " + column + " from " + tableName
                    + " where 1=2";

            rs = statement.executeQuery(queryColumnSql);
            ResultSetMetaData rsMetaData = rs.getMetaData();
            for (int i = 0, len = rsMetaData.getColumnCount(); i < len; i++) {
                columnMetaDataCollection.add(new MysqlColumnMetaData(rsMetaData.getColumnName(i + 1), rsMetaData.getColumnType(i + 1), rsMetaData.getColumnTypeName(i + 1)));
            }
            return columnMetaDataCollection;

        } finally {
            DBUtil.closeDBResources(rs, statement, null);
        }
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
                                               String url, String user, String pass, boolean checkSlave) {
        Connection connection = null;

        try {
            connection = connect(dataBaseType, url, user, pass);
            if (connection != null) {
                if (dataBaseType.equals(DataBaseType.MySql) && checkSlave) {
                    //dataBaseType.MySql
                    return !isSlaveBehind(connection);
                } else {
                    return true;
                }
            }
        } catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        } finally {
            DBUtil.closeDBResources(null, connection);
        }
        return false;
    }

    public static boolean testConnWithoutRetry(DataBaseType dataBaseType,
                                               String url, String user, String pass, List<String> preSql) {
        Connection connection = null;
        try {
            connection = connect(dataBaseType, url, user, pass);
            if (null != connection) {
                for (String pre : preSql) {
                    if (!doPreCheck(connection, pre)) {
                        LOG.warn("doPreCheck failed.");
                        return false;
                    }
                }
                return true;
            }
        } catch (Exception e) {
            LOG.warn("test connection of [{}] failed, for {}.", url,
                    e.getMessage());
        } finally {
            DBUtil.closeDBResources(null, connection);
        }

        return false;
    }

    public static ResultSet query(Connection conn, String sql, List<Object> params)
            throws SQLException {
        PreparedStatement stmt = getStatement(conn, sql, params);
        //默认3600 seconds
        stmt.setQueryTimeout(SOCKET_TIMEOUT_INSECOND);
        return query(stmt);
    }

    private static boolean doPreCheck(Connection conn, String pre) {
        ResultSet rs = null;
        try {
            rs = query(conn, pre, null);

            int checkResult = -1;
            if (DBUtil.asyncResultSetNext(rs)) {
                checkResult = rs.getInt(1);
                if (DBUtil.asyncResultSetNext(rs)) {
                    LOG.warn(
                            "pre check failed. It should return one result:0, pre:[{}].",
                            pre);
                    return false;
                }

            }

            if (0 == checkResult) {
                return true;
            }

            LOG.warn(
                    "pre check failed. It should return one result:0, pre:[{}].",
                    pre);
        } catch (Exception e) {
            LOG.warn("pre check failed. pre:[{}], errorMessage:[{}].", pre,
                    e.getMessage());
        } finally {
            DBUtil.closeResultSet(rs);
        }
        return false;
    }

    // warn:until now, only oracle need to handle session config.
    public static void dealWithSessionConfig(Connection conn,
                                             Configuration config, DataBaseType databaseType, String message) throws SQLException {
        List<String> sessionConfig;
        switch (databaseType) {
            case Oracle:
                sessionConfig = config.getList(SESSION, new ArrayList<>());
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case DRDS:
                // 用于关闭 drds 的分布式事务开关
                sessionConfig = new ArrayList<>();
                sessionConfig.add("set transaction policy 4");
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            case MySql:
                sessionConfig = config.getList(SESSION, new ArrayList<>());
                DBUtil.doDealWithSessionConfig(conn, sessionConfig, message);
                break;
            default:
                break;
        }
    }

    public static void doDealWithSessionConfig(Connection conn,
                                                List<String> sessions, String message) throws SQLException {
        if (null == sessions || sessions.isEmpty()) {
            return;
        }

        Statement stmt;
        stmt = conn.createStatement();

        for (String sessionSql : sessions) {
            LOG.info("execute sql:[{}]", sessionSql);
            DBUtil.executeSqlWithoutResultSet(stmt, sessionSql);
        }
        DBUtil.closeDBResources(stmt, null);
    }

    /**
     * 异步获取resultSet的next(),注意，千万不能应用在数据的读取中。只能用在meta的获取
     *
     * @param resultSet
     * @return
     */
    public static boolean asyncResultSetNext(final ResultSet resultSet) {
        return asyncResultSetNext(resultSet, 3600);
    }

    public static boolean asyncResultSetNext(final ResultSet resultSet, int timeout) {
        Future<Boolean> future = rsExecutors.get().submit(resultSet::next);
        try {
            return future.get(timeout, TimeUnit.SECONDS);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
