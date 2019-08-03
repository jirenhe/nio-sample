package com.wanshifu.transformers.common;

import java.util.HashMap;
import java.util.Map;

public enum EventType {

    INSERT("insert"),

    UPDATE("update"),

    DELETE("delete"),
    ;

    private final String code;

    EventType(String code) {
        this.code = code;
    }

    private static final Map<String, EventType> stringMapping = new HashMap<>((int) (EventType.values().length / 0.75));

    static {
        for (EventType instance : EventType.values()) {
            stringMapping.put(instance.toString(), instance);
        }
    }

    public static EventType fromString(String symbol) {
        return stringMapping.get(symbol);
    }

    @Override
    public String toString() {
        return code;
    }
}
