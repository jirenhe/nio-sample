package com.wanshifu.transformers.common;

public abstract class Event<T> {

    private final long timestamp;

    private final Object target;

    private final T data;

    public Event(Object target, T data) {
        this.target = target;
        this.data = data;
        timestamp = System.currentTimeMillis();
    }

    public Event(Object target) {
        this(target, null);
    }

    public String getType() {
        return this.getClass().getSimpleName();
    }

    public T getData() {
        return data;
    }

    public long getTriggerTime() {
        return timestamp;
    }

    public Object getTarget() {
        return target;
    }
}
