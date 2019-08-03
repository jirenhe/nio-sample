package com.wanshifu.transformers.core.channel.taskpool.event;

import com.wanshifu.transformers.core.channel.taskpool.TaskPersistentException;
import com.wanshifu.transformers.common.Event;

public class PersistentFailEvent extends Event<TaskPersistentException> {

    public PersistentFailEvent(Object target, TaskPersistentException data) {
        super(target, data);
    }
}
