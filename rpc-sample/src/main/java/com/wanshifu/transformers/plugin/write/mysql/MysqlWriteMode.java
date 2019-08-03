package com.wanshifu.transformers.plugin.write.mysql;

import com.wanshifu.transformers.common.UnsupportedTypeException;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.utils.MysqlColumnMetaData;
import com.wanshifu.transformers.common.utils.MysqlColumnMetaDataCollection;
import com.wanshifu.transformers.plugin.write.mysql.processor.PrepareSqlDetail;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Types;
import java.util.*;
import java.util.regex.Pattern;

public enum MysqlWriteMode {

    INSERT("insert") {
        @Override
        public boolean withCondition() {
            return false;
        }

        @Override
        public boolean withColumns() {
            return true;
        }

        @Override
        public PrepareSqlDetail doResolveSqlDetail(String table, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            return new PrepareSqlDetail(genPlaceholderSql("INSERT INTO", table, columns), columns, columnMetaDataCollection);
        }

        @Override
        public String createSql(String table, Record record, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            return genSql("INSERT INTO", table, record, columns, columnMetaDataCollection);
        }
    },

    INSERT_DUPLICATE_UPDATE("insertDuplicateUpdate") {
        @Override
        public boolean withCondition() {
            return false;
        }

        @Override
        public boolean withColumns() {
            return true;
        }

        @Override
        public PrepareSqlDetail doResolveSqlDetail(String table, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            String sql = genPlaceholderSql("INSERT INTO", table, columns);
            StringBuilder sb = new StringBuilder();
            sb.append(" ON DUPLICATE KEY UPDATE ");
            boolean first = true;
            for (String column : columns) {
                if (!first) {
                    sb.append(",");
                } else {
                    first = false;
                }
                sb.append("`").append(column).append("`");
                sb.append("=VALUES(");
                sb.append("`").append(column).append("`");
                sb.append(")");
            }
            return new PrepareSqlDetail(sql + sb.toString(), columns, columnMetaDataCollection);
        }

        @Override
        public String createSql(String table, Record record, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            String sql = genSql("INSERT INTO", table, record, columns, columnMetaDataCollection);
            StringBuilder sb = new StringBuilder();
            sb.append(" ON DUPLICATE KEY UPDATE ");
            boolean first = true;
            for (String columnName : columns) {
                Column column = record.getColumn(columnName);
                if (column != null) {
                    MysqlColumnMetaData columnMetaData = columnMetaDataCollection.get(columnName);
                    if (columnMetaData != null) {
                        if (!first) {
                            sb.append(",");
                        } else {
                            first = false;
                        }
                        sb.append("`").append(columnName).append("`");
                        sb.append("=VALUES(");
                        sb.append("`").append(columnName).append("`");
                        sb.append(")");
                    }
                }
            }
            return sql + sb.toString();
        }
    },

    REPLACE("replace") {
        @Override
        public boolean withCondition() {
            return false;
        }

        @Override
        public boolean withColumns() {
            return true;
        }

        @Override
        public PrepareSqlDetail doResolveSqlDetail(String table, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            return new PrepareSqlDetail(genPlaceholderSql("REPLACE INTO", table, columns), columns, columnMetaDataCollection);
        }

        @Override
        public String createSql(String table, Record record, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            return genSql("REPLACE INTO", table, record, columns, columnMetaDataCollection);
        }
    },

    UPDATE("update") {
        @Override
        public boolean withCondition() {
            return true;
        }

        @Override
        public boolean withColumns() {
            return true;
        }

        @Override
        public PrepareSqlDetail doResolveSqlDetail(String table, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            StringBuilder stringBuilder = new StringBuilder("update ").append(table).append(" set");
            boolean first = true;
            for (String column : columns) {
                if (!first) {
                    stringBuilder.append(",");
                }
                first = false;
                stringBuilder.append(" `").append(column).append("`").append(" = ?");
            }
            List<String> detailColumns = new ArrayList<>(columns.size() + conditionColumns.size());
            detailColumns.addAll(columns);
            detailColumns.addAll(conditionColumns);
            return new PrepareSqlDetail(appendCondition(stringBuilder.toString(), conditionColumns), detailColumns, columnMetaDataCollection);
        }

        @Override
        public String createSql(String table, Record record, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            StringBuilder stringBuilder = new StringBuilder("update ").append(table).append(" set");
            boolean first = true;
            for (String columnName : columns) {
                Column column = record.getColumn(columnName);
                if (column != null) {
                    MysqlColumnMetaData columnMetaData = columnMetaDataCollection.get(columnName);
                    if (columnMetaData != null) {
                        if (!first) {
                            stringBuilder.append(",");
                        } else {
                            first = false;
                        }
                        stringBuilder.append(" `").append(columnName).append("`").append(" = ").append(toSqlValue(column, columnMetaData));
                    }
                }
            }
            return appendCondition(stringBuilder.toString(), conditionColumns, record, columnMetaDataCollection);
        }
    },

    DELETE("delete") {
        @Override
        public boolean withCondition() {
            return true;
        }

        @Override
        public boolean withColumns() {
            return false;
        }

        @Override
        public PrepareSqlDetail doResolveSqlDetail(String table, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            String str = String.format("delete from %s", table);
            return new PrepareSqlDetail(appendCondition(str, conditionColumns), conditionColumns, columnMetaDataCollection);
        }

        @Override
        public String createSql(String table, Record record, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns) {
            String str = String.format("delete from %s", table);
            return appendCondition(str, conditionColumns, record, columnMetaDataCollection);
        }
    },
    ;

    public final String code;

    MysqlWriteMode(String code) {
        this.code = code;
    }

    private static final Map<String, MysqlWriteMode> stringMapping = new HashMap<>((int) (MysqlWriteMode.values().length / 0.75));

    static {
        for (MysqlWriteMode instance : MysqlWriteMode.values()) {
            stringMapping.put(instance.toString(), instance);
        }
    }

    public static MysqlWriteMode fromString(String symbol) {
        return stringMapping.get(symbol);
    }

    @Override
    public String toString() {
        return code;
    }

    private static String genPlaceholderSql(String prefix, String table, Collection<String> columns) {
        String sql = prefix + " %s (%s) VALUES (%s)";
        StringBuilder columnStringBd = new StringBuilder();
        StringBuilder placeholderStringBd = new StringBuilder();
        boolean first = true;
        for (String column : columns) {
            if (!first) {
                columnStringBd.append(",");
                placeholderStringBd.append(",");
            } else {
                first = false;
            }
            columnStringBd.append("`").append(column).append("`");
            placeholderStringBd.append("?");
        }
        return String.format(sql, table, columnStringBd.toString(), placeholderStringBd.toString());
    }

    private static String genSql(String prefix, String table, Record record, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection) {
        String sql = prefix + " %s (%s) VALUES (%s)";
        StringBuilder columnStringBd = new StringBuilder();
        StringBuilder values = new StringBuilder();
        boolean first = true;
        for (String columnName : columns) {
            Column column = record.getColumn(columnName);
            if (column != null) {
                MysqlColumnMetaData columnMetaData = columnMetaDataCollection.get(columnName);
                if (columnMetaData != null) {
                    if (!first) {
                        columnStringBd.append(",");
                        values.append(",");
                    } else {
                        first = false;
                    }
                    columnStringBd.append("`").append(column.getName()).append("`");
                    values.append(toSqlValue(column, columnMetaData));
                }
            }
        }
        return String.format(sql, table, columnStringBd.toString(), values.toString());
    }

    private static String appendCondition(String target, List<String> columns) {
        StringBuilder stringBuilder = new StringBuilder(target).append(" where 1=1 ");
        for (String conditionColumn : columns) {
            stringBuilder.append("and ").append("`").append(conditionColumn).append("`").append(" = ? ");
        }
        return stringBuilder.toString();
    }

    private static String appendCondition(String target, List<String> columns, Record record, MysqlColumnMetaDataCollection columnMetaDataCollection) {
        StringBuilder stringBuilder = new StringBuilder(target).append(" where 1=1 ");
        for (String conditionColumn : columns) {
            Column column = record.getColumn(conditionColumn);
            MysqlColumnMetaData columnMetaData;
            if (column == null) {
                throw new RuntimeException(String.format("条件字段%s在record中不存在！请检查配置！ record:%s", conditionColumn, record.toString()));
            }
            if ((columnMetaData = columnMetaDataCollection.get(column.getName())) == null) {
                throw new RuntimeException(String.format("条件字段%s在目标表中不存在！请检查配置", conditionColumn));
            }
            stringBuilder.append("and ").append("`").append(conditionColumn).append("`").append(" = ").append(toSqlValue(column, columnMetaData));
        }
        return stringBuilder.toString();
    }

    public PrepareSqlDetail resolveSqlDetail(String table, List<String> columns, MysqlColumnMetaDataCollection metaDataCollection, List<String> conditionColumns) throws IllegalArgumentException {
        if (this.withCondition() && CollectionUtils.isEmpty(conditionColumns)) {
            throw new IllegalArgumentException("this mode need condition columns!");
        }
        if (this.withCondition() && CollectionUtils.isEmpty(conditionColumns)) {
            throw new IllegalArgumentException("this mode need columns!");
        }
        return doResolveSqlDetail(table, columns, metaDataCollection, conditionColumns);
    }

    private static String toSqlValue(Column column, MysqlColumnMetaData columnMetaData) {
        if (column == null || column.getValue() == null) {
            return "null";
        } else {
            String value = doTypeMapping(columnMetaData, column);
            value = filterSensitiveChar(value);
            return "'" + value + "'";
        }
    }

    private static String filterSensitiveChar(String value) {
        return value.replaceAll("(['\\\\])", "\\\\$1");
    }

    public abstract boolean withColumns();

    public abstract boolean withCondition();

    public abstract PrepareSqlDetail doResolveSqlDetail(String table, List<String> columns, MysqlColumnMetaDataCollection columnMetaDataCollection, List<String> conditionColumns);

    public abstract String createSql(String table, Record record, List<String> columns, MysqlColumnMetaDataCollection metaDataCollection, List<String> conditionColumns);

    private static String doTypeMapping(MysqlColumnMetaData columnMetaData, Column column) {
        Date utilDate;
        switch (columnMetaData.getSqlType()) {
            case Types.CHAR:
            case Types.NCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
                return column.asString();

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.TINYINT:
                return column.asLong().toString();

            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return column.asBigDecimal().toString();

            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (columnMetaData.getSourceType().equalsIgnoreCase("year")) {
                    return column.asBigInteger().intValue() + "";
                } else {
                    utilDate = column.asDate();
                    java.sql.Date sqlDate = null;
                    if (null != utilDate) {
                        sqlDate = new java.sql.Date(utilDate.getTime());
                    }
                    assert sqlDate != null;
                    return sqlDate.toString(); // format yyyy-mm-dd.
                }

            case Types.TIME:
                java.sql.Time sqlTime = null;
                utilDate = column.asDate();
                if (null != utilDate) {
                    sqlTime = new java.sql.Time(utilDate.getTime());
                }
                assert sqlTime != null;
                return sqlTime.toString();  //format hh:mm:ss.

            case Types.TIMESTAMP:
                java.sql.Timestamp sqlTimestamp = null;
                utilDate = column.asDate();
                if (null != utilDate) {
                    sqlTimestamp = new java.sql.Timestamp(utilDate.getTime());
                }
                assert sqlTimestamp != null;
                return sqlTimestamp.toString(); //format yyyy-mm-dd hh:mm:ss.fffffffff

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return new String(column.asBytes());

            case Types.BOOLEAN:
                return column.asString();

            // warn: bit(1) -> Types.BIT 可使用setBoolean
            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
            case Types.BIT:
                return column.asLong().intValue() + "";
            default:
                throw new UnsupportedTypeException(
                        String.format(
                                "不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 可以尝试使用sql函数转型 .",
                                columnMetaData.getColumnName(),
                                columnMetaData.getSourceType()));
        }
    }

    public static void main(String[] args) {
        String str = "asdqqwe\\\\zxa'asd\\as/asd;asd,as";
        System.out.println(str);
        System.out.println(filterSensitiveChar(str));
    }
}
