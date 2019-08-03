package com.wanshifu.transformers.common;

import java.util.HashMap;
import java.util.Map;

public enum State {

    INIT("init"),

    RUNNING("running"),

    SHUTDOWN("shutdown"),
    ;

    public final String code;

    State(String code) {
        this.code = code;
    }

    private static final Map<String, State> stringMapping = new HashMap<>((int) (State.values().length / 0.75));

    static {
        for (State instance : State.values()) {
            stringMapping.put(instance.toString(), instance);
        }
    }

    public static State fromString(String symbol) {
        return stringMapping.get(symbol);
    }

    @Override
    public String toString() {
        return code;
    }
}
