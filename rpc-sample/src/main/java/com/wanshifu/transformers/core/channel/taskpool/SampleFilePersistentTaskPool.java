package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.queue.ArrayRingQueue;
import com.wanshifu.transformers.common.utils.queue.RingQueue;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class SampleFilePersistentTaskPool extends AbstractFilePersistentTaskPool implements TaskPool {

    private final ConcurrentMap<String, ChannelTask> taskMap = new ConcurrentHashMap<>(1024);

    private RingQueue<File> fileRingQueue;

    private static final String FILE_SEPARATOR = File.separator;

    private static final int BUFFER_SIZE = 1024 * 512; //缓冲区大小，默认512KB

    private static final int DEFAULT_FILE_COUNT = 2; //默认文件数量

    private static final Logger LOGGER = LoggerFactory.getLogger(SampleFilePersistentTaskPool.class);

    private static final Schema<ProtobuffListWrapper> schema = RuntimeSchema.getSchema(ProtobuffListWrapper.class);

    public SampleFilePersistentTaskPool(Context channelContext) {
        super(channelContext);
    }

    @Override
    protected void postInit() throws InitException {
        Context context = getContext();
        initFile(context);
        try {
            Collection<ChannelTask> channelTaskCollection = load();
            for (ChannelTask channelTask : channelTaskCollection) {
                taskMap.put(channelTask.getTaskId(), channelTask);
            }
        } catch (Throwable e) {
            throw new InitException("task load fail!", e);
        }
    }

    @Override
    public int size() {
        return taskMap.size();
    }

    @Override
    public void shutdown() {
        try {
            persistentAll();
        } catch (TaskPersistentException e) {
            LOGGER.error("task persistent fail!", e);
        }
    }

    @Override
    public Collection<ChannelTask> getAll() {
        return taskMap.values();
    }

    @Override
    protected void enPool(ChannelTask channelTask) throws TaskPersistentException {
        taskMap.put(channelTask.getTaskId(), channelTask);
        persistentAll();
    }

    @Override
    protected void dePool(ChannelTask channelTask) throws TaskPersistentException {
        taskMap.remove(channelTask.getTaskId());
        persistentAll();
    }


    private void persistentAll() throws TaskPersistentException {
        this.persistent(this.taskMap.values());
    }

    private void initFile(Context context) throws InitException {
        //文件数量
        int fileCount = context.getConfig().getInt(CoreConstants.TASK_POOL_PARAMETER_FILE_COUNT, DEFAULT_FILE_COUNT);
        String filePrefix = "channel-id-" + context.getName() + "-";
        List<File> files = new ArrayList<>(fileCount);
        for (int i = 0; i < fileCount; i++) {
            files.add(new File(dirPath + FILE_SEPARATOR + filePrefix + "channelTasks" + i + FILE_EXTENSION));
        }
        fileRingQueue = new ArrayRingQueue<>(files);
    }

    private Collection<ChannelTask> load() {
        List<File> fileList = new ArrayList<>(fileRingQueue.getAll());
        fileList.sort((o1, o2) -> {
            long time1 = 0, time2 = 0;
            if (o1.exists()) {
                time1 = o1.lastModified();
            }
            if (o2.exists()) {
                time2 = o2.lastModified();
            }
            return Long.compare(time1, time2);
        });
        Collection<ChannelTask> channelTaskCollection = Collections.emptyList();
        for (int i = fileList.size() - 1; i > 0; i--) {
            File file = fileList.get(i);
            if (!file.exists()) {
                break;
            }
            try {
                channelTaskCollection = doLoad(file);
                LOGGER.info("load task size : {}", channelTaskCollection.size());
                return channelTaskCollection;
            } catch (Exception e) {
                LOGGER.warn("load task from file " + file.getName() + " fail!", e);
            }
        }
        return channelTaskCollection;
    }


    private void persistent(Collection<ChannelTask> channelTasks) throws TaskPersistentException {
        ArrayList<ChannelTask> target = new ArrayList<>(channelTasks);
        ProtobuffListWrapper wrapper = new ProtobuffListWrapper(target);
        File file = fileRingQueue.getAndMove();
        byte[] objectBytes = toBytes(wrapper, schema, BUFFER_SIZE);
        writeFile(objectBytes, file);
    }

    private Collection<ChannelTask> doLoad(File file) throws Exception {
        return readFromFile(file, schema).getChannelTasks();
    }


    private static class ProtobuffListWrapper {

        private List<ChannelTask> channelTasks;

        public ProtobuffListWrapper() {
        }

        public ProtobuffListWrapper(List<ChannelTask> channelTasks) {
            this.channelTasks = channelTasks;
        }

        public List<ChannelTask> getChannelTasks() {
            return channelTasks;
        }

        public void setChannelTasks(List<ChannelTask> channelTasks) {
            this.channelTasks = channelTasks;
        }

    }
}
