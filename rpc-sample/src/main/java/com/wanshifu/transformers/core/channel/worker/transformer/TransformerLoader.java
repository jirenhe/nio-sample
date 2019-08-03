package com.wanshifu.transformers.core.channel.worker.transformer;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.UnexpectedException;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class TransformerLoader {

    private static final String CLASS = "class";

    public static List<Transformer> loadTransForm(List<Configuration> configurations) throws ConfigurationException {
        if (configurations == null || configurations.size() == 0) {
            return Collections.emptyList();
        }
        List<Transformer> transformers = new ArrayList<>(configurations.size());
        for (Configuration configuration : configurations) {
            transformers.add(parseConfiguration(configuration));
        }
        return transformers;
    }

    private static Transformer parseConfiguration(Configuration configuration) throws ConfigurationException {
        String type = configuration.getNecessaryValue(CLASS);
        String className = "com.wanshifu.transformers.core.channel.worker.transformer." + type;
        try {
            Class<?> clazz = Class.forName(className);
            if (Transformer.class.isAssignableFrom(clazz)) {
                Transformer transformer = (Transformer) clazz.newInstance();
                if (transformer instanceof Configurable) {
                    ((Configurable) transformer).setConfiguration(configuration);
                }
                return transformer;
            } else {
                throw new ConfigurationException("unknown transformer class for " + type);
            }
        } catch (ClassNotFoundException e) {
            throw new ConfigurationException("unknown transformer class for " + type);
        } catch (IllegalAccessException | InstantiationException e) {
            throw new UnexpectedException("reflection constructor create transformer object fail!");
        }
    }
}
