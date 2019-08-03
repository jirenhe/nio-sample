package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.ContextAware;
import com.wanshifu.transformers.common.UnexpectedException;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.common.utils.StringUtils;
import com.wanshifu.transformers.core.channel.taskpool.limit.ThresholdLimitTaskPoolProxy;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class TaskPoolFactory {

    private final static Map<String, Class<? extends TaskPool>> map = new HashMap<>();

    static {
        map.put("sampleFile", SampleFilePersistentTaskPool.class);
        map.put("multiFile", MultiFilePersistentTaskPool.class);
        map.put("randomAccessFile", RandomAccessFilePersistentTaskPool.class);
        map.put("noPersistent", NoPersistentTaskPool.class);
    }

    public static TaskPool getTaskPool(Context context) throws ConfigurationException {
        Configuration configuration = context.getConfig();
        String strategy = configuration.getString(CoreConstants.TASK_POOL_STRATEGY);
        TaskPool taskPool;
        if (configuration.getConfiguration(CoreConstants.TASK_POOL) == null || StringUtils.isEmpty(strategy)) {
            taskPool = new NoPersistentTaskPool(context);
        } else {
            Class<? extends TaskPool> clazz = map.get(strategy);
            if (clazz == null) {
                throw new ConfigurationException("taskPool strategy " + strategy + " is unrecognized!");
            }
            try {
                if (AbstractTaskPool.class.isAssignableFrom(clazz)) {
                    taskPool = clazz.getConstructor(Context.class).newInstance(context);
                } else {
                    taskPool = clazz.newInstance();
                }
            } catch (InstantiationException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new UnexpectedException(e);
            }
        }
        return resolveLimitProxy(context, taskPool);
    }

    private static TaskPool resolveLimitProxy(Context context, TaskPool taskPool) throws ConfigurationException {
        Configuration configuration = context.getConfig();
        TaskPool result = taskPool;
        if (configuration.getConfiguration(CoreConstants.TASK_POOL_LIMIT) != null) {
            boolean fair = configuration.getBool(CoreConstants.TASK_POOL_LIMIT_FAIR, false);
            int threshold = configuration.getInt(CoreConstants.TASK_POOL_LIMIT_THRESHOLD, Integer.MAX_VALUE);
            result = new ThresholdLimitTaskPoolProxy(taskPool, threshold, fair);
            ((ContextAware) result).setContext(context);
        }
        return result;
    }
}
