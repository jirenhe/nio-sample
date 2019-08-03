package com.wanshifu.transformers.plugin.reader.stream;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.Column;
import com.wanshifu.transformers.common.bean.Record;
import com.wanshifu.transformers.common.bean.StringColumn;
import com.wanshifu.transformers.common.bean.Task;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.core.job.transport.Transport;
import com.wanshifu.transformers.plugin.reader.Reader;

import java.util.ArrayList;
import java.util.List;

public class StreamReader extends Configurable implements Reader {

    private static final String LOOP_COUNT = "loopCount";

    private static final String COLUMN = "column";

    private List<Column> columns;

    private long loopCount;

    private boolean shutDownFlag = false;

    @Override
    public void init() throws InitException {
        Configuration configuration = getConfiguration();
        Configuration parameterConfig = configuration.getConfiguration(CoreConstants.PARAMETER);
        loopCount = parameterConfig.getLong(LOOP_COUNT, 100);
        List<Configuration> columnConfigs = parameterConfig.getListConfiguration(COLUMN);
        if (columnConfigs.size() == 0) {
            throw new ConfigurationException("column config is missing!");
        }
        columns = new ArrayList<>(columnConfigs.size());
        for (Configuration columnConfig : columnConfigs) {
            columns.add(new StringColumn(columnConfig.get(CoreConstants.NAME), columnConfig.getString(CoreConstants.VALUE), "string", false));
        }
    }

    @Override
    public void startReader(Transport transport) {
        for (long i = 0; i < loopCount && !shutDownFlag; i++) {
            Task task = new Task("streamReader", "null", "streamReader", EventType.INSERT, buildRecord());
            transport.send(task);
        }
        if (!shutDownFlag) {
            transport.done();
        }
    }

    private Record buildRecord() {
        Record record = new Record();
        for (Column column : columns) {
            record.addColumn(column);
        }
        return record;
    }

    @Override
    public void destroy() {
        shutDownFlag = true;
    }

}
