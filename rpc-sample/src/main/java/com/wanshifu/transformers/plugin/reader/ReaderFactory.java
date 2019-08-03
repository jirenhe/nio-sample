package com.wanshifu.transformers.plugin.reader;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.UnexpectedException;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.plugin.reader.cannal.CanalReader;
import com.wanshifu.transformers.plugin.reader.kafka.KafkaReader;
import com.wanshifu.transformers.plugin.reader.mysql.MySqlReader;
import com.wanshifu.transformers.plugin.reader.stream.StreamReader;

import java.util.HashMap;
import java.util.Map;

public class ReaderFactory {

    private final static Map<String, Class<? extends Reader>> readerMap = new HashMap<>();

    static {
        readerMap.put("mysql", MySqlReader.class);
        readerMap.put("kafka", KafkaReader.class);
        readerMap.put("canal", CanalReader.class);
        readerMap.put("streamReader", StreamReader.class);
    }

    private ReaderFactory() {
    }

    public static Reader getReader(Configuration configuration) throws ConfigurationException {
        String name = configuration.getNecessaryValue(CoreConstants.NAME);
        Class<? extends Reader> readerClazz = readerMap.get(name);
        if (readerClazz == null) {
            throw new ConfigurationException("reader name " + name + " is unrecognized!");
        }
        Reader reader;
        try {
            reader = readerClazz.newInstance();
            if (reader instanceof Configurable) {
                ((Configurable) reader).setConfiguration(configuration);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnexpectedException(e);
        }
        return reader;
    }
}
