package com.wanshifu.transformers.core.channel.taskpool.event;

import com.wanshifu.transformers.common.Event;

public class TaskDePoolEvent extends Event<Integer> {

    public TaskDePoolEvent(Object target, Integer integer) {
        super(target, integer);
    }
}
