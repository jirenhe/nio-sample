package com.wanshifu.transformers.core.alarm.strategy;

import com.wanshifu.transformers.core.alarm.IAlarm;

/**
 * @author ：Smile(wangyajun)
 * @date ：Created in 2019/5/29 11:29
 * @description： 默认告警 什么都不做
 */
public class DefaultAlermStrategy implements IAlarm {
    @Override
    public void init() {
        //do nothing
    }

    @Override
    public void send(String content) {
        //do nothing
    }
}
