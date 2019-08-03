package com.wanshifu.transformers.core.channel.worker.transformer;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.wanshifu.transformers.GlobalConfig;
import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.core.channel.worker.transformer.classloader.DynamicGroovyClassLoader;
import com.wanshifu.transformers.core.channel.worker.transformer.classloader.DynamicJavaClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

import java.io.File;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DynamicCodeTransformer extends Configurable implements Transformer {

    private Transformer codeTransformer;

    private ClassLoader classLoader;

    private final static Logger LOGGER = LoggerFactory.getLogger(DynamicCodeTransformer.class);

    @Override
    public void init() throws InitException {
        Configuration configuration = this.getConfiguration();
        Objects.requireNonNull(configuration);
        String dynamicCodePath = configuration.getNecessaryValue(CoreConstants.DYNAMIC_CODE_PATH);
        File dir = new File(dynamicCodePath);
        if (!dir.exists() || !dir.isDirectory()) {
            throw new ConfigurationException(CoreConstants.DYNAMIC_CODE_PATH + " is not a dir or not exists!");
        }
        String dynamicCodeClass = configuration.getString(CoreConstants.DYNAMIC_CODE_CLASS, "CodeTransformer");
        Configuration parameterConfiguration = configuration.getConfiguration(CoreConstants.PARAMETER);
        String style = resolveStyle(dynamicCodePath, dynamicCodeClass);
        try {
            if ("GROOVY".equalsIgnoreCase(style)) {
                classLoader = new DynamicGroovyClassLoader(dynamicCodePath, dynamicCodePath);
            } else if ("JAVA".equalsIgnoreCase(style)) {
                classLoader = new DynamicJavaClassLoader(dynamicCodePath, dynamicCodePath);
            }
            Class<?> clazz = classLoader.loadClass(dynamicCodeClass);
            Object o = clazz.newInstance();
            if (o instanceof Transformer) {
                if (parameterConfiguration != null && o instanceof ParameterAware) {
                    JSONObject parameter = JSON.parseObject(parameterConfiguration.toJSON());
                    resolveParameterPlaceholder(parameter);
                    ((ParameterAware) o).setParameter(parameter);
                }
                this.codeTransformer = (Transformer) o;
                this.codeTransformer.init();
            } else {
                throw new InitException(CoreConstants.DYNAMIC_CODE_CLASS + " must implements Transformer interface!");
            }
            LOGGER.info("load dynamic code successful! class is {}", clazz.getSimpleName());
        } catch (Exception e) {
            throw new InitException(e);
        }

    }

    private void resolveParameterPlaceholder(JSONObject parameter) {
        Pattern pattern = Pattern.compile("^\\$\\{(\\S+)+}$");
        Configuration globalConfig = GlobalConfig.getGlobalConfig();
        for (Map.Entry<String, Object> stringObjectEntry : parameter.entrySet()) {
            Object tmp = stringObjectEntry.getValue();
            if (tmp instanceof String) {
                String value = (String) tmp;
                Matcher matcher = pattern.matcher(value);
                if (matcher.matches()) {
                    String placeholder = matcher.group(1);
                    Object object = globalConfig.get(placeholder);
                    if (object instanceof Configuration) {
                        object = JSON.parseObject(((Configuration) object).toJSON());
                    }
                    stringObjectEntry.setValue(object);
                }
            }
        }
    }

    private String resolveStyle(String dynamicCodePath, String dynamicCodeClass) throws ConfigurationException {
        String path = dynamicCodePath;
        if (!dynamicCodePath.endsWith(File.separator)) {
            path += File.separator;
        }
        path += dynamicCodeClass;
        File groovyFile = new File(path + ".groovy");
        File javaFile = new File(path + ".java");
        if (groovyFile.exists()) {
            return "GROOVY";
        } else if (javaFile.exists()) {
            return "JAVA";
        } else {
            throw new ConfigurationException(String.format("%s config class is not find! : [%s]", CoreConstants.DYNAMIC_CODE_CLASS, dynamicCodeClass));
        }
    }

    @Override
    public void shutdown() {
        codeTransformer.shutdown();
    }

    @Override
    public void doTransform(ChannelTask channelTask) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("before code transform record is : {}", channelTask.getRecord());
        }
        Thread.currentThread().setContextClassLoader(classLoader);

        String groovyFileName = codeTransformer.getClass().getSimpleName();
        MDC.put("logFileName",groovyFileName);
        codeTransformer.doTransform(channelTask);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("after code transform record is : {}", channelTask.getRecord());
        }
    }
}
