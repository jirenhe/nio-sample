package com.wanshifu.transformers.common;

public class RuntimeExceptionEvent extends Event<Exception> {

    private final boolean shutdown;

    public RuntimeExceptionEvent(Object target, Exception data) {
        super(target, data);
        this.shutdown = false;
    }

    public RuntimeExceptionEvent(Object target, Exception data, boolean shutdown) {
        super(target, data);
        this.shutdown = shutdown;
    }

    public boolean isShutdown() {
        return shutdown;
    }
}
