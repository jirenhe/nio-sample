package com.wanshifu.transformers.common.constant;

public class CoreConstants {

    public static final String CONFIG_JOB_JSON = "jobJson";

    public static final String CONFIG_CONFIG_PATH = "configPath";

    public static final String CONFIG_LOG_CONFIG = "log";

    public static final String DEFAULT_LOG_FILE_NAME = "logback.xml";

    public static final String DEFAULT_CONFIG_JSON_FILE_NAME = "job.json";

    public static final String DEFAULT_CONFIG_CODE_PATH = "code";

    public static final String CHANNELS = "channels";

    public static final String READER_PLUGIN = "readerPlugin";

    public static final String NAME = "name";

    public static final String VALUE = "value";

    public static final String TYPE = "type";

    public static final String PARAMETER = "parameter";

    public static final String CHANNEL_ID = "id";

    public static final String EXTRA_DATE_FORMAT = "extra.dateFormat";

    public static final String CHANNEL_WORK_SIZE = "workSize";

    public static final String CHANNEL_MONITOR_WORKER_DETAIL = "monitorWorkerDetail";

    public static final String CHANNEL_RETRY_TIMES = "retryTimes";

    public static final String WORKER = "worker";

    public static final String WORKER_WRITE_PLUGIN = "worker.writePlugin";

    public static final String WORKER_WRITE_EXECUTOR = "worker.writeExecutor";

    public static final String WORKER_WRITE_EXECUTOR_TYPE = "worker.writeExecutor.type";

    public static final String WORKER_WRITE_EXECUTOR_BUFFER_SIZE = "worker.writeExecutor.bufferSize";

    public static final String WORKER_WRITE_EXECUTOR_POLLING_TIME = "worker.writeExecutor.pollingTime";

    public static final String WORKER_TRANSFORMER = "worker.transformer";

    public static final String WORKER_TRANSFORMER_TYP = "worker.transformer.type";

    public final static String DYNAMIC_CODE_PATH = "dynamicCodePath";

    public final static String DYNAMIC_CODE_CLASS = "dynamicCodeClass";

    public static final String TASK_POOL = "taskPool";

    public static final String TASK_POOL_STRATEGY = "taskPool.strategy";

    public static final String TASK_POOL_PARAMETER = "taskPool.parameter";

    public static final String TASK_POOL_PARAMETER_PATH = "taskPool.parameter.path";

    public static final String TASK_POOL_PARAMETER_MAX_FILE_SIZE = "taskPool.parameter.maxFileSize";

    public static final String TASK_POOL_PARAMETER_CLEAR_INTERVAL = "taskPool.parameter.clearInterval";

    public static final String TASK_POOL_PARAMETER_FILE_COUNT = "taskPool.parameter.fileCount";

    public static final String TASK_POOL_PERSISTENT = "taskPool.persistent";

    public static final String TASK_POOL_LIMIT = "taskPool.limit";

    public static final String TASK_POOL_LIMIT_THRESHOLD = "taskPool.limit.threshold";

    public static final String TASK_POOL_LIMIT_FAIR = "taskPool.limit.fair";

    public static final String LOAD_BALANCE = "loadBalance";

    public static final String LOAD_BALANCE_TYPE = "loadBalance.type";

    public static final String LOAD_BALANCE_PARAMETER = "loadBalance.parameter";

    public static final String ALARM = "alarm";

    // ----------------------------- 环境变量 ---------------------------------
    public static String HOME_PATH = System.getProperty("HOME_PATH");
}
