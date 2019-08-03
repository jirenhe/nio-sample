package com.wanshifu.transformers.core.channel.taskpool;

public class TaskPersistentException extends Exception {

    public TaskPersistentException() {
    }

    public TaskPersistentException(String message) {
        super(message);
    }

    public TaskPersistentException(String message, Throwable cause) {
        super(message, cause);
    }

    public TaskPersistentException(Throwable cause) {
        super(cause);
    }
}
