package com.wanshifu.transformers.common;

import com.wanshifu.transformers.common.utils.Configuration;

public abstract class Configurable {

    private Configuration configuration;

    public Configuration getConfiguration() {
        return configuration;
    }

    public void setConfiguration(Configuration configuration) {
        this.configuration = configuration;
    }
}
