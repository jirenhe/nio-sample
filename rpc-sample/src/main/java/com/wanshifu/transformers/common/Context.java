package com.wanshifu.transformers.common;

import com.wanshifu.transformers.common.utils.Configuration;

public interface Context {

    Configuration getConfig();

    String getName();

    void registerEventListener(EventListener<?> eEventListener);

    void publishEvent(Event event);

    State getState();
}