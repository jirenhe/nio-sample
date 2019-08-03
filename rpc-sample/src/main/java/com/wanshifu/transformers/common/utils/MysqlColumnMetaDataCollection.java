package com.wanshifu.transformers.common.utils;

import java.sql.Types;
import java.util.*;

public class MysqlColumnMetaDataCollection extends AbstractList<MysqlColumnMetaData> {

    private final Map<String, MysqlColumnMetaData> map;

    public MysqlColumnMetaDataCollection() {
        this.map = new LinkedHashMap<>();
    }

    @Override
    public boolean add(MysqlColumnMetaData columnMetaData) {
        map.put(columnMetaData.getColumnName(), columnMetaData);
        return true;
    }

    @Override
    public MysqlColumnMetaData remove(int index) {
        Set<Map.Entry<String, MysqlColumnMetaData>> entrySet = map.entrySet();
        checkBound(index, map.entrySet());
        int i = 0;
        for (Map.Entry<String, MysqlColumnMetaData> stringColumnMetaDataEntry : entrySet) {
            if (i == index) {
                return map.remove(stringColumnMetaDataEntry.getKey());
            }
            i++;
        }
        return null;
    }

    public MysqlColumnMetaData get(String column) {
        return map.get(column);
    }

    public Collection<String> getColumns() {
        return map.keySet();
    }

    @Override
    public MysqlColumnMetaData get(int index) {
        Set<Map.Entry<String, MysqlColumnMetaData>> entrySet = map.entrySet();
        checkBound(index, entrySet);
        int i = 0;
        for (Map.Entry<String, MysqlColumnMetaData> stringColumnMetaDataEntry : entrySet) {
            if (i == index) {
                return stringColumnMetaDataEntry.getValue();
            }
            i++;
        }
        return null;
    }

    @Override
    public int size() {
        return map.size();
    }

    private void checkBound(int index, Collection c) {
        if (index < 0 || index >= c.size()) {
            throw new IndexOutOfBoundsException();
        }
    }

    public static void main(String[] args) {
        MysqlColumnMetaDataCollection mysqlColumnMetaDataCollection = new MysqlColumnMetaDataCollection();
        mysqlColumnMetaDataCollection.add(new MysqlColumnMetaData("f1", Types.BIGINT, "aaaa"));
        mysqlColumnMetaDataCollection.add(new MysqlColumnMetaData("f2", Types.DATE, "date"));
        mysqlColumnMetaDataCollection.add(new MysqlColumnMetaData("f3", Types.BLOB, "ccc"));
        mysqlColumnMetaDataCollection.add(new MysqlColumnMetaData("f1", Types.BIGINT, "ddd"));

        System.out.println(mysqlColumnMetaDataCollection.getColumns());
        System.out.println(mysqlColumnMetaDataCollection.get(0));
        System.out.println(mysqlColumnMetaDataCollection.get("f1"));

        System.out.println("----loop");
        for (MysqlColumnMetaData mysqlColumnMetaDatum : mysqlColumnMetaDataCollection) {
            System.out.println(mysqlColumnMetaDatum);
        }

        mysqlColumnMetaDataCollection.remove(2);
        System.out.println("----loop");

        for (MysqlColumnMetaData mysqlColumnMetaDatum : mysqlColumnMetaDataCollection) {
            System.out.println(mysqlColumnMetaDatum);
        }

    }

}
