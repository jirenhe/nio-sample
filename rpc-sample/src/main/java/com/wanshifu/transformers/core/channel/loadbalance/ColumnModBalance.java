package com.wanshifu.transformers.core.channel.loadbalance;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.common.utils.StringUtils;
import com.wanshifu.transformers.core.channel.worker.Worker;

import java.util.Arrays;
import java.util.List;

/**
 * 通过列值取模负载，可以保证对于同一条记录，每次进入都分配到同一个worker
 */
public class ColumnModBalance extends LoadBalance {

    private final int length;

    private final String[] columnNames;

    private static final String PARAMETER_COLUMN_NAMES = "columnNames";

    public ColumnModBalance(Context context, List<Worker> workers) throws ConfigurationException {
        super(context, workers);
        length = workers.size();
        Configuration configuration = context.getConfig().getConfiguration(CoreConstants.LOAD_BALANCE_PARAMETER);
        String configStr = configuration.getNecessaryValue(PARAMETER_COLUMN_NAMES);
        this.columnNames = configStr.split(",");
        if (Arrays.stream(columnNames).anyMatch(StringUtils::isEmpty)) {
            throw new ConfigurationException(CoreConstants.LOAD_BALANCE_PARAMETER + PARAMETER_COLUMN_NAMES + " is invalid");
        }
    }

    @Override
    public void proxy(ChannelTask channelTask) {
        int hashCode = 0;
        for (String columnName : columnNames) {
            Column column = channelTask.getRecord().getColumn(columnName);
            if (column != null && column.getValue() != null) {
                hashCode += column.getValue().hashCode();
            }
        }
        int index = Math.abs(hashCode) % length;
        workers.get(index).execute(channelTask);
    }
}
