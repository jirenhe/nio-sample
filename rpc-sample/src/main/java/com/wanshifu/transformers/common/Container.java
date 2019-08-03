package com.wanshifu.transformers.common;

public interface Container {

    void init() throws InitException;

    void start();

    void shutdown();

    String monitorInfo();
}
