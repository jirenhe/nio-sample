package com.wanshifu.transformers.common.utils;

import com.wanshifu.transformers.common.bean.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;

public class ColumnFactory {

    public static Column createFromStringByMysqlColumnMeta(MysqlColumnMetaData columnMetaData, String value, boolean isPrimaryKey) {
        String sourceType = columnMetaData.getSourceType();
        int sqlType = columnMetaData.getSqlType();
        String columnName = columnMetaData.getColumnName();
        switch (sqlType) {

            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.NULL:
                return new StringColumn(columnName, value, sourceType, isPrimaryKey);

            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BOOLEAN:
            case Types.BIT:
                return new LongColumn(columnName, value, sourceType, isPrimaryKey);

            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return new BigDecimalColumn(columnName, value, sourceType, isPrimaryKey);

            case Types.TIME:
                return new DateColumn(columnName, value, "HH:mm:ss", sourceType, isPrimaryKey);

            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (sourceType.equalsIgnoreCase("year")) {
                    return new LongColumn(columnName, value, sourceType, isPrimaryKey);
                } else {
                    return new DateColumn(columnName, value, sourceType, isPrimaryKey);
                }

            case Types.TIMESTAMP:
                return new DateColumn(columnName, value, sourceType, isPrimaryKey);

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return new BytesColumn(columnName, value == null ? null : value.getBytes(), sourceType, isPrimaryKey);

            default:
                return new ObjectColumn(columnName, value, sourceType, isPrimaryKey);
        }
    }

    public static Column createFromResultSetByMysqlColumnMeta(MysqlColumnMetaData columnMetaData, ResultSet rs, boolean isPrimaryKey) throws SQLException {
        String sourceType = columnMetaData.getSourceType();
        int sqlType = columnMetaData.getSqlType();
        String columnName = columnMetaData.getColumnName();
        switch (sqlType) {

            case Types.CHAR:
            case Types.NCHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
            case Types.NVARCHAR:
            case Types.LONGNVARCHAR:
            case Types.CLOB:
            case Types.NCLOB:
            case Types.NULL:
                return new StringColumn(columnName, rs.getString(columnName), sourceType, isPrimaryKey);

            case Types.SMALLINT:
            case Types.TINYINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.BOOLEAN:
            case Types.BIT:
                return new LongColumn(columnName, rs.getLong(columnName), sourceType, isPrimaryKey);

            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                return new BigDecimalColumn(columnName, rs.getBigDecimal(columnName), sourceType, isPrimaryKey);

            case Types.TIME:
                return new DateColumn(columnName, rs.getTime(columnName), sourceType, isPrimaryKey);

            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (sourceType.equalsIgnoreCase("year")) {
                    return new LongColumn(columnName, rs.getInt(columnName), sourceType, isPrimaryKey);
                } else {
                    return new DateColumn(columnName, rs.getDate(columnName), sourceType, isPrimaryKey);
                }

            case Types.TIMESTAMP:
                return new DateColumn(columnName, rs.getTimestamp(columnName), sourceType, isPrimaryKey);

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                return new BytesColumn(columnName, rs.getBytes(columnName), sourceType, isPrimaryKey);

            default:
                return new ObjectColumn(columnName, rs.getObject(columnName), sourceType, isPrimaryKey);
        }
    }
}
