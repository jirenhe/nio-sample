package com.wanshifu.transformers.core.alarm;

import com.wanshifu.transformers.common.InitException;

/**
 * @author ：Smile(wangyajun)
 * @date ：Created in 2019/5/29 11:26
 * @description：告警接口
 */
public interface IAlarm {

    public void init () throws InitException;

    /**
     * 发送告警信息
     * @param content 告警文本信息
     */
    public void send(String content);



}
