package com.wanshifu.transformers.common;

import com.wanshifu.transformers.common.utils.Configuration;

import java.util.Objects;

public abstract class AbstractConfigAbleContext implements Context{

    private final Configuration configuration;

    public AbstractConfigAbleContext(Configuration configuration) {
        Objects.requireNonNull(configuration);
        this.configuration = configuration;
    }

    @Override
    public Configuration getConfig() {
        return configuration;
    }
}
