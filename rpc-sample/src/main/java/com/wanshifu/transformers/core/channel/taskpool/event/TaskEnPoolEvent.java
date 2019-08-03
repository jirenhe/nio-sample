package com.wanshifu.transformers.core.channel.taskpool.event;

import com.wanshifu.transformers.common.Event;

public class TaskEnPoolEvent extends Event<Integer> {

    public TaskEnPoolEvent(Object target, Integer integer) {
        super(target, integer);
    }
}
