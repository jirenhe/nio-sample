package com.wanshifu.transformers.common;

@FunctionalInterface
public interface EventListener<E extends Event> {

    void onEvent(E event);

}
