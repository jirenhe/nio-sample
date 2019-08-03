package com.wanshifu.transformers.plugin.write.mysql.processor;

import com.wanshifu.transformers.common.Configurable;
import com.wanshifu.transformers.common.EventType;
import com.wanshifu.transformers.common.utils.*;
import com.wanshifu.transformers.plugin.write.mysql.MysqlWriteMode;
import com.wanshifu.transformers.plugin.write.mysql.WriterDigest;
import org.apache.commons.collections.CollectionUtils;

import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

public class AbstractMysqlEventProcessor extends Configurable {

    protected static final String COLUMNS = "columns";

    protected static final String CONDITION_COLUMNS = "conditionColumns";

    protected static final String WRITER_MODE = "writerMode";

    protected final EventType eventType;

    protected MysqlWriteMode mysqlWriteMode;

    protected final WriterDigest writerDigest;

    protected List<String> conditionColumns;

    protected List<String> columns;

    protected MysqlColumnMetaDataCollection columnsMetaData;

    protected AbstractMysqlEventProcessor(EventType eventType, Configuration configuration, WriterDigest writerDigest) throws ConfigurationException {
        this.eventType = eventType;
        this.setConfiguration(configuration);
        this.writerDigest = writerDigest;
        String tmp = configuration.getNecessaryValue(WRITER_MODE);
        MysqlWriteMode mode = MysqlWriteMode.fromString(tmp);
        this.conditionColumns = configuration.getList(CONDITION_COLUMNS);
        if (mode == null) {
            throw new ConfigurationException("配置mode值不正确！");
        } else {
            mysqlWriteMode = mode;
        }
        resolveResultSetMetaData();
    }

    private void resolveResultSetMetaData() throws ConfigurationException {
        Set<String> columns = new LinkedHashSet<>();
        try {
            this.columnsMetaData = DBUtil.getColumnMetaData(DataBaseType.MySql,
                    writerDigest.getUrl(), writerDigest.getUserName(), writerDigest.getPassword(), writerDigest.getTable(), "*");
        } catch (SQLException e) {
            throw new ConfigurationException("read meta data fail!", e);
        }

        Configuration configuration = getConfiguration();
        List<String> defineColumns = configuration.getList(COLUMNS);
        if (CollectionUtils.isEmpty(defineColumns) || (defineColumns.size() == 1 && "*".equals(defineColumns.get(0)))) {
            columns.addAll(this.columnsMetaData.getColumns());
        } else {
            columns.addAll(defineColumns);
        }

        //如果配置了排除字段 那么就等于直接取*然后减去相关排除字段即可
        List<String> columnExcludeList = configuration.getList("excludeColumns");
        if (CollectionUtils.isNotEmpty(columnExcludeList)) {
            this.columns = columns.stream().filter(s -> !columnExcludeList.contains(s)).collect(Collectors.toList());
        } else {
            this.columns = new ArrayList<>(columns);
        }
        if (CollectionUtils.isEmpty(this.columns)) {
            throw new ConfigurationException("配置目标表的列为空行，请检查配置！");
        }
        //这里可以检查columns是否所有的元素都在columnsMetaData里面
        Optional<String> optional = this.columns.stream().filter(s -> columnsMetaData.get(s) == null).findAny();
        if (optional.isPresent()) {
            throw new ConfigurationException(String.format("配置的列：%s，在目标表：%s中不存在！请检查配置！", optional.get(), writerDigest.getTable()));
        }
    }
}
