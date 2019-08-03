package com.wanshifu.transformers.core.alarm;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.EventListener;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.RuntimeExceptionEvent;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.core.channel.taskpool.event.PersistentFailEvent;
import com.wanshifu.transformers.core.channel.worker.event.TaskExecuteFailEvent;
import org.apache.commons.lang3.exception.ExceptionUtils;

/**
 * @author ：Smile(wangyajun)
 * @date ：Created in 2019/5/29 14:14
 * @description：
 */
public class AlarmContainer {

    private IAlarm alarm;

    private final Context context;


    public AlarmContainer(Context context) {
        this.context = context;
    }

    public void init() throws InitException {
        //1 注册监听事件，
        context.registerEventListener(new TaskExecuteFailEventListener());
        context.registerEventListener(new RuntimeExceptionEventListener());
        context.registerEventListener(new PersistentFailEventListener());
        //2并且初始化警告需要的信息
        Configuration alarmConfig = context.getConfig().getConfiguration(CoreConstants.ALARM);
        alarm = AlarmFactory.getAlarm(alarmConfig);
        alarm.init();
    }


    private class TaskExecuteFailEventListener implements EventListener<TaskExecuteFailEvent> {
        @Override
        public void onEvent(TaskExecuteFailEvent event) {
            //失败事件 告警
            StringBuilder builder = new StringBuilder();
            for (ChannelTask datum : event.getData()) {
                builder.append("失败告警taskId:").append(datum.getTaskId()).append("|");
                Record record = datum.getRecord();
                builder.append("失败告警记录:").append(record.toString());
            }
            builder.append("\r\n\t").append(ExceptionUtils.getStackTrace(event.getException()));
            alarm.send(builder.toString());
        }
    }

    private class RuntimeExceptionEventListener implements EventListener<RuntimeExceptionEvent> {

        @Override
        public void onEvent(RuntimeExceptionEvent event) {
            //失败事件 告警
            StringBuilder builder = new StringBuilder();
            builder.append("\r\n\t").append(ExceptionUtils.getStackTrace(event.getData()));
            alarm.send(builder.toString());
        }
    }

    private class PersistentFailEventListener implements EventListener<PersistentFailEvent> {

        @Override
        public void onEvent(PersistentFailEvent event) {
            //失败事件 告警
            StringBuilder builder = new StringBuilder();
            builder.append("\r\n\t").append(ExceptionUtils.getStackTrace(event.getData()));
            alarm.send(builder.toString());
        }
    }


}
