package com.wanshifu.transformers.plugin.write;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.UnexpectedException;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.plugin.write.es.EsWriter;
import com.wanshifu.transformers.plugin.write.mysql.MySqlWriter;
import com.wanshifu.transformers.plugin.write.none.NoOpWriter;
import com.wanshifu.transformers.plugin.write.stream.StreamWriter;

import java.util.HashMap;
import java.util.Map;

public class WriterFactory {

    private final static Map<String, Class<? extends Writer>> writerMap = new HashMap<>();

    static {
        writerMap.put("mysql", MySqlWriter.class);
        writerMap.put("es", EsWriter.class);
        writerMap.put("streamWriter", StreamWriter.class);
        writerMap.put("noOp", NoOpWriter.class);
    }

    private WriterFactory() {
    }

    public static Writer getWrite(Configuration configuration) throws ConfigurationException {
        String name = configuration.getNecessaryValue(CoreConstants.NAME);
        Class<? extends Writer> writerClazz = writerMap.get(name);
        if (writerClazz == null) {
            throw new ConfigurationException("writer name " + name + " is unrecognized!");
        }
        Writer writer;
        try {
            writer = writerClazz.newInstance();
            if (writer instanceof Configurable) {
                ((Configurable) writer).setConfiguration(configuration);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnexpectedException(e);
        }
        return writer;
    }
}
