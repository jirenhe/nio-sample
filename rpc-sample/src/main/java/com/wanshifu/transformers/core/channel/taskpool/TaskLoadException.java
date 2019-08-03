package com.wanshifu.transformers.core.channel.taskpool;

public class TaskLoadException extends Exception {

    public TaskLoadException() {
    }

    public TaskLoadException(String message) {
        super(message);
    }

    public TaskLoadException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskLoadException(Throwable cause) {
        super(cause);
    }
}
