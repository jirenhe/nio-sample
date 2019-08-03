package com.wanshifu.transformers.common.bean;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.io.Serializable;
import java.util.*;

public class Record implements Serializable, Cloneable {

    private Map<String, Column> columns;

    private Map<String, Object> datas;

    private Set<Column> primaryKeys;

    private Date tmpTime;

    public Record() {
        columns = new LinkedHashMap<>(13);
        primaryKeys = new HashSet<>(4);
        datas = new HashMap<>(4);
    }

    public Column getColumn(String columnName) {
        return columns.get(columnName);
    }

    public Record addColumn(Column column) {
        columns.put(column.getName(), column);
        if (column.isPrimaryKey()) {
            primaryKeys.add(column);
        }
        return this;
    }

    public Column remove(Column column) {
        return columns.remove(column.getName());
    }

    public Column remove(String columnName) {
        return columns.remove(columnName);
    }

    public Date getTmpTime() {
        return tmpTime;
    }

    public void setTmpTime(Date tmpTime) {
        this.tmpTime = tmpTime;
    }

    public Object getData(String key) {
        return datas.get(key);
    }

    public Record addData(String key, Object value) {
        datas.put(key, value);
        return this;
    }

    public Set<Column> getPrimaryKeys() {
        return primaryKeys;
    }

    public Map<String, Column> getColumns() {
        return columns;
    }

    public Map<String, Object> getDatas() {
        return datas;
    }

    @Override
    @SuppressWarnings("unchecked")
    public Record clone() {
        Record record;
        try {
            record = (Record) super.clone();
            record.columns = new HashMap<>((int) (this.columns.size() / 0.75));
            record.primaryKeys = new HashSet<>((int) (this.primaryKeys.size() / 0.75));
            for (Map.Entry<String, Column> entry : columns.entrySet()) {
                record.addColumn(entry.getValue().clone());
            }
            record.datas = (Map<String, Object>) ((HashMap) this.datas).clone();
        } catch (CloneNotSupportedException e) {
            throw new AssertionError("this should never happened!");
        }
        return record;
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this, SerializerFeature.PrettyFormat);
    }
}
