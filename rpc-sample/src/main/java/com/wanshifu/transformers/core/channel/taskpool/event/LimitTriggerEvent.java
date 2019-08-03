package com.wanshifu.transformers.core.channel.taskpool.event;

import com.wanshifu.transformers.common.Event;

public class LimitTriggerEvent extends Event<Object> {


    public LimitTriggerEvent(Object target) {
        super(target);
    }
}
