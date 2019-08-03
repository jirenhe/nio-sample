package com.wanshifu.transformers.common.utils;

import lombok.Getter;

@Getter
public class MysqlColumnMetaData {

    private final String columnName;

    private final int sqlType;

    private final String sourceType;

    public MysqlColumnMetaData(String columnName, int sqlType, String sourceType) {
        this.columnName = columnName;
        this.sqlType = sqlType;
        this.sourceType = sourceType;
    }

    @Override
    public String toString() {
        return "MysqlColumnMetaData{" +
                "columnName='" + columnName + '\'' +
                ", sqlType=" + sqlType +
                ", sourceType='" + sourceType + '\'' +
                '}';
    }
}
