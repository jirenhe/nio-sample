package com.wanshifu.transformers.common.bean;

import com.wanshifu.transformers.common.EventType;
import lombok.Getter;
import lombok.Setter;

import java.io.Serializable;

@Getter
public class Task implements Serializable {

    private final String dataSource;

    private final String db;

    private final String table;

    private final EventType eventType;

    @Getter
    @Setter
    private Record record;

    public Task(String dataSource, String db, String table, EventType eventType, Record record) {
        this.dataSource = dataSource;
        this.db = db;
        this.table = table;
        this.eventType = eventType;
        this.record = record;
    }

    public Task(Task task) {
        this.dataSource = task.dataSource;
        this.db = task.db;
        this.table = task.table;
        this.eventType = task.eventType;
        this.record = task.record;
    }
}
