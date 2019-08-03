package com.wanshifu.transformers.core.alarm;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.UnexpectedException;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.core.alarm.strategy.DefaultAlermStrategy;
import com.wanshifu.transformers.core.alarm.strategy.MailAlermStrategy;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ：Smile(wangyajun)
 * @date ：Created in 2019/5/29 13:59
 * @description：
 */
public class AlarmFactory {


    private final static Map<String, Class<? extends IAlarm>> alarmMap = new HashMap<>();

    static {
        alarmMap.put("email", MailAlermStrategy.class);
        alarmMap.put("default", DefaultAlermStrategy.class);
//        readerMap.put("dingTalk", CanalReader.class);
//        readerMap.put("phone", StreamReader.class);
    }

    private AlarmFactory() {
    }

    public static IAlarm getAlarm(Configuration configuration) throws ConfigurationException {
        String name;
        if (null == configuration) {
            name = "default";
        } else {
            name = configuration.getUnnecessaryValue(CoreConstants.NAME, "default");
        }
        Class<? extends IAlarm> alarmClazz = alarmMap.get(name);
        if (alarmClazz == null) {
            throw new ConfigurationException("reader name " + name + " is unrecognized!");
        }
        IAlarm alarm;
        try {
            alarm = alarmClazz.newInstance();
            if (alarm instanceof Configurable) {
                ((Configurable) alarm).setConfiguration(configuration);
            }
        } catch (InstantiationException | IllegalAccessException e) {
            throw new UnexpectedException(e);
        }
        return alarm;
    }

}
