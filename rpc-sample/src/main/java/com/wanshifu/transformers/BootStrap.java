package com.wanshifu.transformers;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.joran.JoranConfigurator;
import ch.qos.logback.core.joran.spi.JoranException;
import ch.qos.logback.core.util.StatusPrinter;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.common.utils.StringUtils;
import com.wanshifu.transformers.core.job.JobContainer;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

public class BootStrap {

    public static void main(String[] args) {
        try {
            if (args == null || args.length == 0) {
                throw new IllegalArgumentException("需要启动参数！");
            }
            Configuration configuration = init(args);
            GlobalConfig.setConfig(configuration);
            start(configuration);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static Configuration init(String[] args) throws Exception {
        ConfigOption configOption = ConfigOption.parse(args);
        parserLogConfig(configOption);
        return parserJobConfig(configOption);
    }

    private static void start(Configuration configuration) throws InitException {
        JobContainer jobContainer = new JobContainer(configuration);
        jobContainer.init();
        Runtime.getRuntime().addShutdownHook(new Thread(jobContainer::shutdown));
        jobContainer.start();
    }

    private static void parserLogConfig(ConfigOption configOption) throws JoranException {
        if (StringUtils.isEmpty(System.getProperty("LOG_HOME")) && configOption.configDir != null) {
            System.setProperty("LOG_HOME", configOption.configDir.getAbsolutePath());
        }
        File file = null;
        if (configOption.configLogFile != null) {
            file = configOption.configLogFile;
        } else {
            if (configOption.configDir != null) {
                file = findFile(configOption.configDir, CoreConstants.DEFAULT_LOG_FILE_NAME);
            }
        }
        if (file != null) {
            LoggerContext lc = (LoggerContext) LoggerFactory.getILoggerFactory();
            JoranConfigurator configurator = new JoranConfigurator();
            configurator.setContext(lc);
            lc.reset();
            configurator.doConfigure(file);
            StatusPrinter.printInCaseOfErrorsOrWarnings(lc);
        }
    }

    private static Configuration parserJobConfig(ConfigOption configOption) throws ConfigurationException, IOException {
        File file = configOption.jobJsonFile;
        File dynamicCodePath = null;
        if (file == null) {
            if (configOption.configDir != null) {
                file = findFile(configOption.configDir, CoreConstants.DEFAULT_CONFIG_JSON_FILE_NAME);
                dynamicCodePath = findFile(configOption.configDir, CoreConstants.DEFAULT_CONFIG_CODE_PATH);
            }
        }
        if (file != null && file.isFile()) {
            String json = new String(Files.readAllBytes(file.toPath()));
            Configuration configuration = Configuration.from(json);
            if (dynamicCodePath != null && dynamicCodePath.exists() && dynamicCodePath.isDirectory()) {
                completeDynamicCodePath(configuration, dynamicCodePath);
            }
            configuration.set(CoreConstants.CONFIG_CONFIG_PATH, configOption.configDir == null ? null : configOption.configDir.getAbsolutePath());
            configuration.set(CoreConstants.CONFIG_JOB_JSON, configOption.jobJsonFile == null ? null : configOption.jobJsonFile.getAbsolutePath());
            return configuration;
        } else {
            throw new IllegalArgumentException("jobJson未配置或者文件未找到！");
        }
    }

    private static void completeDynamicCodePath(Configuration configuration, File dynamicCodePath) throws ConfigurationException {
        List<Configuration> channels = configuration.getListConfiguration(CoreConstants.CHANNELS);
        for (int i = 0; i < channels.size(); i++) {
            Configuration channel = channels.get(i);
            List<Configuration> transformerConfigs = channel.getListConfiguration(CoreConstants.WORKER_TRANSFORMER);
            for (int j = 0; j < transformerConfigs.size(); j++) {
                String path = CoreConstants.CHANNELS + "[" + i + "]." + CoreConstants.WORKER_TRANSFORMER + "[" + j + "]." + CoreConstants.DYNAMIC_CODE_PATH;
                configuration.set(path, dynamicCodePath);
            }
        }
    }

    private static File findFile(File dir, String fileName) {
        if (dir != null) {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                Optional<File> logFile = Arrays.stream(files).filter(f -> f.getName().equals(fileName)).findFirst();
                if (logFile.isPresent()) {
                    return logFile.get();
                }
            }
        }
        return null;
    }

    private static final class ConfigOption {

        private File jobJsonFile;

        private File configLogFile;

        private File configDir;

        public static ConfigOption parse(String[] args) throws Exception {
            Options options = new Options();
            options.addOption(CoreConstants.CONFIG_JOB_JSON, true, "Job config.");
            options.addOption(CoreConstants.CONFIG_CONFIG_PATH, true, "Job config.");
            options.addOption(CoreConstants.CONFIG_LOG_CONFIG, true, "Job config.");

            DefaultParser parser = new DefaultParser();
            CommandLine cl = parser.parse(options, args);
            ConfigOption opption = new ConfigOption();
            String jobJson = cl.getOptionValue(CoreConstants.CONFIG_JOB_JSON);
            String configPath = cl.getOptionValue(CoreConstants.CONFIG_CONFIG_PATH);
            String configLogPath = cl.getOptionValue(CoreConstants.CONFIG_LOG_CONFIG);
            if (StringUtils.isNotEmpty(configPath)) {
                File f = new File(configPath);
                if (!f.exists() || !f.isDirectory()) {
                    throw new IllegalArgumentException(String.format("配置的%s目录错误：%s", CoreConstants.CONFIG_CONFIG_PATH, configPath));
                }
                opption.configDir = f;
            }
            if (StringUtils.isNotEmpty(jobJson)) {
                File f = new File(jobJson);
                if (!f.exists() || !f.isFile()) {
                    throw new IllegalArgumentException(String.format("配置的%s文件错误：%s", CoreConstants.CONFIG_JOB_JSON, jobJson));
                }
                opption.jobJsonFile = f;
            }
            if (StringUtils.isNotEmpty(configLogPath)) {
                File f = new File(configLogPath);
                if (!f.exists() || !f.isFile()) {
                    throw new IllegalArgumentException(String.format("配置的%s文件错误：%s", CoreConstants.CONFIG_LOG_CONFIG, configLogPath));
                }
                opption.configLogFile = f;
            }
            return opption;
        }
    }
}
