package com.wanshifu.transformers.plugin.write.mysql.processor;

import com.wanshifu.transformers.common.UnsupportedTypeException;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.bean.ObjectColumn;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.utils.MysqlColumnMetaData;
import com.wanshifu.transformers.common.utils.MysqlColumnMetaDataCollection;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.Collection;
import java.util.Date;

public class PrepareSqlDetail {

    private final String preparedSql;

    private final Collection<String> fillColumns;

    private final MysqlColumnMetaDataCollection columnsMetaData;

    public PrepareSqlDetail(String preparedSql, Collection<String> fillColumns, MysqlColumnMetaDataCollection columnsMetaData) {
        this.preparedSql = preparedSql;
        this.fillColumns = fillColumns;
        this.columnsMetaData = columnsMetaData;
    }

    public void fillStatement(PreparedStatement preparedStatement, Record record) throws SQLException {
        int i = 0;
        for (String fillColumn : fillColumns) {
            MysqlColumnMetaData columnMetaData = columnsMetaData.get(fillColumn);
            Column column = record.getColumn(fillColumn);
            if (column == null || column.getValue() == null) {
                preparedStatement.setObject(i + 1, null);
            } else {
                if (column instanceof ObjectColumn) {
                    preparedStatement.setObject(i + 1, column.getValue());
                } else {
                    doTypeMapping(preparedStatement, i, columnMetaData, column);
                }
            }
            i++;
        }
    }

    private void doTypeMapping(PreparedStatement preparedStatement, int i, MysqlColumnMetaData columnMetaData, Column column) throws SQLException {
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
                preparedStatement.setString(i + 1, column.asString());
                break;

            case Types.BIGINT:
                preparedStatement.setLong(i + 1, column.asLong());
                break;

            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.TINYINT:
                preparedStatement.setInt(i + 1, column.asLong().intValue());
                break;

            case Types.NUMERIC:
            case Types.DECIMAL:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
                preparedStatement.setBigDecimal(i + 1, column.asBigDecimal());
                break;

            // for mysql bug, see http://bugs.mysql.com/bug.php?id=35115
            case Types.DATE:
                if (columnMetaData.getSourceType().equalsIgnoreCase("year")) {
                    preparedStatement.setInt(i + 1, column.asBigInteger().intValue());
                } else {
                    java.sql.Date sqlDate = null;
                    utilDate = column.asDate();
                    if (null != utilDate) {
                        sqlDate = new java.sql.Date(utilDate.getTime());
                    }
                    preparedStatement.setDate(i + 1, sqlDate);
                }
                break;

            case Types.TIME:
                java.sql.Time sqlTime = null;
                utilDate = column.asDate();
                if (null != utilDate) {
                    sqlTime = new java.sql.Time(utilDate.getTime());
                }
                preparedStatement.setTime(i + 1, sqlTime);
                break;

            case Types.TIMESTAMP:
                java.sql.Timestamp sqlTimestamp = null;
                utilDate = column.asDate();
                if (null != utilDate) {
                    sqlTimestamp = new java.sql.Timestamp(utilDate.getTime());
                }
                preparedStatement.setTimestamp(i + 1, sqlTimestamp);
                break;

            case Types.BINARY:
            case Types.VARBINARY:
            case Types.BLOB:
            case Types.LONGVARBINARY:
                preparedStatement.setBytes(i + 1, column.asBytes());
                break;

            case Types.BOOLEAN:
                preparedStatement.setString(i + 1, column.asString());
                break;

            // warn: bit(1) -> Types.BIT 可使用setBoolean
            // warn: bit(>1) -> Types.VARBINARY 可使用setBytes
            case Types.BIT:
                preparedStatement.setInt(i + 1, column.asLong().intValue());
                break;
            default:
                throw new UnsupportedTypeException(
                        String.format(
                                "不支持数据库写入这种字段类型. 字段名:[%s], 字段类型:[%s]. 可以尝试使用sql函数转型 .",
                                columnMetaData.getColumnName(),
                                columnMetaData.getSourceType()));
        }
    }

    public String getPreparedSql() {
        return preparedSql;
    }
}
