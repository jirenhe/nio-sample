package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class MultiFilePersistentTaskPool extends AbstractFilePersistentTaskPool implements TaskPool {

    private final ConcurrentMap<String, TaskFileMapping> taskMap = new ConcurrentHashMap<>(1024);

    private static final int BUFFER_SIZE = 512; //缓冲区大小，默认512字节

    private static final Logger LOGGER = LoggerFactory.getLogger(MultiFilePersistentTaskPool.class);

    public MultiFilePersistentTaskPool(Context channelContext) {
        super(channelContext);
    }

    @Override
    protected void postInit() throws InitException {
        try {
            List<TaskFileMapping> taskCollection = load();
            for (TaskFileMapping taskFileMapping : taskCollection) {
                taskMap.put(taskFileMapping.channelTask.getTaskId(), taskFileMapping);
            }
        } catch (Throwable e) {
            throw new InitException("channelTask load fail!", e);
        }
    }

    @Override
    public int size() {
        return taskMap.size();
    }

    @Override
    public void shutdown() {

    }

    @Override
    public Collection<ChannelTask> getAll() {
        List<ChannelTask> channelTasks = new ArrayList<>(taskMap.size());
        for (TaskFileMapping value : taskMap.values()) {
            channelTasks.add(value.channelTask);
        }
        return channelTasks;
    }

    @Override
    protected void enPool(ChannelTask channelTask) throws TaskPersistentException {
        String id = channelTask.getTaskId();
        if (!taskMap.containsKey(id)) {
            taskMap.put(channelTask.getTaskId(), new TaskFileMapping(channelTask, persistent(channelTask)));
        }
    }

    @Override
    protected void dePool(ChannelTask channelTask) throws TaskPersistentException {
        String id = channelTask.getTaskId();
        TaskFileMapping taskFileMapping = taskMap.get(id);
        if (taskFileMapping != null) {
            try {
                if (!taskFileMapping.file.delete()) {
                    throw new TaskPersistentException("file delete fail!" + taskFileMapping.file.getAbsolutePath());
                }
            } finally {
                taskMap.remove(id);
            }
        } else {
            //如果taskId不唯一会出现这种情况
            LOGGER.warn("channelTask not in pool!");
        }
    }

    private List<TaskFileMapping> load() {
        File[] files = dir.listFiles();
        if (files == null) {
            return Collections.emptyList();
        }
        List<TaskFileMapping> result = new ArrayList<>();
        for (File file : files) {
            try {
                ChannelTask channelTask = doLoad(file);
                result.add(new TaskFileMapping(channelTask, file));
            } catch (Exception e) {
                LOGGER.warn("load channelTask from file " + file.getName() + " fail!", e);
            }
        }
        if (result.size() > 0) {
            LOGGER.info("load from files channelTask size : {}", result.size());
        }
        return result;
    }


    private File persistent(ChannelTask channelTask) throws TaskPersistentException {
        String id = channelTask.getTaskId();
        File file = new File(dir + File.separator + id + FILE_EXTENSION);
        byte[] objectBytes = toBytes(channelTask, TASK_SCHEMA, BUFFER_SIZE);
        writeFile(objectBytes, file);
        return file;
    }

    private ChannelTask doLoad(File file) throws Exception {
        return readFromFile(file, TASK_SCHEMA);
    }

    private class TaskFileMapping {

        private final ChannelTask channelTask;

        private final File file;

        public TaskFileMapping(ChannelTask channelTask, File file) {
            this.channelTask = channelTask;
            this.file = file;
        }
    }

}
