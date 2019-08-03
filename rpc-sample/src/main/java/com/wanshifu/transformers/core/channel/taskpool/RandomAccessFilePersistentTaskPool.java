package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 并发文件持久化，持久化消耗性能小于1毫秒。通过随机文件访问（RandomAccessFile）实现
 * 存储的格式是 位置信息(9个字节)+任务本身序列化后的字节数
 */
public class RandomAccessFilePersistentTaskPool extends AbstractFilePersistentTaskPool implements TaskPool {

    private final ConcurrentMap<String, TaskFileMapping> taskMap = new ConcurrentHashMap<>(1024);

    private static final int BUFFER_SIZE = 512; //缓冲区大小，默认512字节

    private static final int DEFAULT_MAX_FILE_SIZE = 64 * MB; //默认单文件最大字节数（512MB)

    private static final int POSITION_INFO_BYTE_SIZE = 9; //任务字节位置信息本身占9个字节 起始位int(4) + 末位int(4) + 删除标志1

    private static final Logger LOGGER = LoggerFactory.getLogger(RandomAccessFilePersistentTaskPool.class);

    private int maxFileSize;

    private volatile File currentFile;

    private volatile RandomAccessFile currentRandomAccessFile;

    private volatile int nextFileNo;

    private final FileCleaner fileCleaner = new FileCleaner();

    public RandomAccessFilePersistentTaskPool(Context context) {
        super(context);
    }

    @Override
    protected void postInit() throws InitException {
        Context context = getContext();
        int configMaxFileSize = context.getConfig().getInt(CoreConstants.TASK_POOL_PARAMETER_MAX_FILE_SIZE, 0);
        int clearInterval = context.getConfig().getInt(CoreConstants.TASK_POOL_PARAMETER_CLEAR_INTERVAL, 1000 * 60 * 5);
        if (configMaxFileSize > 0) {
            if (configMaxFileSize > 1024) {
                throw new ConfigurationException("config max file size must less than 1GB");
            }
            maxFileSize = configMaxFileSize * MB;
        } else {
            maxFileSize = DEFAULT_MAX_FILE_SIZE;
        }
        try {
            File[] files = dir.listFiles();
            if (files != null && files.length > 0) {
                for (File file : files) {
                    long fileLength = file.length();
                    int count = load(file);
                    if (fileLength < maxFileSize && count > 0) {
                        currentFile = file;
                        currentRandomAccessFile = new RandomAccessFile(file, "rw");
                        String name = file.getName();
                        nextFileNo = Integer.parseInt(name.split("\\.")[0]) + 1;
                    } else if (count == 0) {
                        LOGGER.info("file " + file.getName() + " was useless, so delete!");
                        file.delete();
                    }
                    if (count < 0) {
                        LOGGER.warn("file " + file.getName() + " was damage, so delete!");
                        file.delete();
                    }
                }
            }
            if (currentFile == null) {
                nextFileNo = 0;
                newFile();
            }
        } catch (Throwable e) {
            throw new InitException("channelTask load fail!", e);
        }
        fileCleaner.setInterval(clearInterval);
        fileCleaner.start();
    }

    @Override
    protected void enPool(ChannelTask channelTask) throws TaskPersistentException {
        String id = channelTask.getTaskId();
        if (taskMap.put(channelTask.getTaskId(), persistent(channelTask)) != null) {
            LOGGER.warn("find duplicate channelTask id : {}", id);
        }
    }


    @Override
    protected void dePool(ChannelTask channelTask) throws TaskPersistentException {
        String id = channelTask.getTaskId();
        TaskFileMapping taskFileMapping = taskMap.remove(id);
        if (taskFileMapping != null) {
            try {
                erasureTask(taskFileMapping);
            } catch (IOException e) {
                throw new TaskPersistentException(e);
            }
        } else {
            //如果taskId不唯一会出现这种情况
            LOGGER.warn("channelTask not in pool!");
        }
    }

    @Override
    public int size() {
        return taskMap.size();
    }

    @Override
    public void shutdown() {
        fileCleaner.shutdown();
        closeCurrent();
    }

    @Override
    public Collection<ChannelTask> getAll() {
        List<ChannelTask> channelTasks = new ArrayList<>(taskMap.size());
        for (TaskFileMapping value : taskMap.values()) {
            channelTasks.add(value.channelTask);
        }
        return channelTasks;
    }

    private TaskFileMapping persistent(ChannelTask channelTask) throws TaskPersistentException {
        byte[] bytes = toBytes(channelTask, TASK_SCHEMA, BUFFER_SIZE);
        int beginPosition, endPosition;
        Position position;
        int objectBytesLength = bytes.length;
        try {
            long time = System.currentTimeMillis();
            TaskFileMapping taskFileMapping;
            synchronized (this) {
                taskFileMapping = new TaskFileMapping(channelTask, currentFile);
                int appendBegin = (int) currentRandomAccessFile.length();
                beginPosition = appendBegin + POSITION_INFO_BYTE_SIZE;
                endPosition = beginPosition + objectBytesLength;
                position = new Position(beginPosition, endPosition);
                byte[] positionInfo = position.toBytes();
                currentRandomAccessFile.seek(appendBegin);
                currentRandomAccessFile.write(positionInfo);
                currentRandomAccessFile.write(bytes);
                if (endPosition >= maxFileSize) {
                    newFile();
                }
            }
            LOGGER.debug("file write take time : {}", System.currentTimeMillis() - time);
            taskFileMapping.position = position;
            return taskFileMapping;
        } catch (IOException e) {
            throw new TaskPersistentException(e);
        }
    }

    private void erasureTask(TaskFileMapping taskFileMapping) throws IOException {
        Position position = taskFileMapping.position;
        boolean useCurrent = false;
        //这里判断任务所属文件是否是当前文件，如果是者直接使用当前文件。（避免创建RandomAccessFile实例，比较消耗性能）
        if (taskFileMapping.file == currentFile) { //双重检查，第一次避免获取锁消耗
            synchronized (this) {
                if (taskFileMapping.file == currentFile) {
                    useCurrent = true;
                    RandomAccessFile randomAccessFile = currentRandomAccessFile;
                    randomAccessFile.seek(position.begin - 1);
                    randomAccessFile.write(-1);
                    randomAccessFile.seek(randomAccessFile.length());
                }
            }
        }
        if (!useCurrent) {
            try (RandomAccessFile randomAccessFile = new RandomAccessFile(taskFileMapping.file, "rw")) {
                randomAccessFile.seek(position.begin - 1);
                randomAccessFile.write(-1);
            }
        }
    }

    private void newFile() throws IOException {
        String name = nextFileNo + FILE_EXTENSION;
        File file = new File(dir + File.separator + name);
        closeCurrent();
        nextFileNo++;
        currentRandomAccessFile = new RandomAccessFile(file, "rw");
        currentFile = file;
        LOGGER.debug("making new file " + name);
    }

    private void closeCurrent() {
        synchronized (this) {
            if (currentFile != null) {
                try {
                    currentRandomAccessFile.close();
                } catch (IOException e) {
                    LOGGER.error("close current file fail!", e);
                }
            }
        }
    }

    private int load(File file) {
        long fileLength = file.length();
        if (fileLength <= 0) {
            return -1;
        }
        byte[] positionBytes = new byte[POSITION_INFO_BYTE_SIZE];
        boolean isDamage = false;
        int count = 0;
        long lastPos = 0;
        try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
            do {
                randomAccessFile.seek(lastPos);
                randomAccessFile.read(positionBytes);
                Position position = Position.fromBytes(positionBytes);
                lastPos = position.end;
                if (position.isDelete) {
                    continue;
                }
                byte[] objectBytes = new byte[position.end - position.begin];
                randomAccessFile.seek(position.begin);
                randomAccessFile.read(objectBytes);
                ChannelTask channelTask = getFromBytes(objectBytes, TASK_SCHEMA);
                if (channelTask != null) {
                    TaskFileMapping taskFileMapping = new TaskFileMapping(channelTask, file, position);
                    taskMap.put(channelTask.getTaskId(), taskFileMapping);
                    count++;
                }
            } while (lastPos < fileLength);
        } catch (Exception e) {
            isDamage = true;
            LOGGER.warn("load channelTask from file " + file.getName() + " fail! position : {}, file length : {}", lastPos, fileLength, e);
        }
        if (count > 0) {
            LOGGER.info("load from file {} channelTask size : {}", file.getName(), count);
        }
        return isDamage ? -1 : count;
    }


    private static class TaskFileMapping {

        private final ChannelTask channelTask;

        private final File file;

        private Position position;

        public TaskFileMapping(ChannelTask channelTask, File file) {
            this.channelTask = channelTask;
            this.file = file;
        }

        public TaskFileMapping(ChannelTask channelTask, File file, Position position) {
            this.channelTask = channelTask;
            this.file = file;
            this.position = position;
        }

        public TaskFileMapping(ChannelTask channelTask, File file, int beginPosition, int endPosition) {
            this.channelTask = channelTask;
            this.file = file;
            this.position = new Position(beginPosition, endPosition);
        }
    }

    private static class Position {

        private final int begin;

        private final int end;

        private final boolean isDelete;

        public Position(int begin, int end) {
            this.begin = begin;
            this.end = end;
            isDelete = false;
        }

        public Position(int begin, int end, boolean isDelete) {
            this.begin = begin;
            this.end = end;
            this.isDelete = isDelete;
        }

        public byte[] toBytes() {
            byte[] positionInfo = new byte[POSITION_INFO_BYTE_SIZE];
            positionInfo[3] = (byte) ((begin >> 24) & 0xFF);
            positionInfo[2] = (byte) ((begin >> 16) & 0xFF);
            positionInfo[1] = (byte) ((begin >> 8) & 0xFF);
            positionInfo[0] = (byte) (begin & 0xFF);
            positionInfo[7] = (byte) ((end >> 24) & 0xFF);
            positionInfo[6] = (byte) ((end >> 16) & 0xFF);
            positionInfo[5] = (byte) ((end >> 8) & 0xFF);
            positionInfo[4] = (byte) (end & 0xFF);
            positionInfo[8] = isDelete ? (byte) -1 : (byte) 1;
            return positionInfo;
        }

        public static Position fromBytes(byte[] src) {
            int begin = (src[0] & 0xFF)
                    | ((src[1] & 0xFF) << 8)
                    | ((src[2] & 0xFF) << 16)
                    | ((src[3] & 0xFF) << 24);
            int end = (src[4] & 0xFF)
                    | ((src[5] & 0xFF) << 8)
                    | ((src[6] & 0xFF) << 16)
                    | ((src[7] & 0xFF) << 24);
            boolean isDelete = src[8] == -1;
            return new Position(begin, end, isDelete);
        }
    }

    private class FileCleaner extends TimerTask {

        private final Timer timer = new Timer();

        private long interval = 1000 * 60 * 5; //5分钟

        @Override
        public void run() {
            File[] files = dir.listFiles();
            if (files == null || files.length == 0) {
                return;
            }
            byte[] bytes = new byte[POSITION_INFO_BYTE_SIZE];
            for (File file : files) {
                long fileLength = file.length();
                if (fileLength >= maxFileSize) {
                    boolean deleteFlag = true;
                    try (RandomAccessFile randomAccessFile = new RandomAccessFile(file, "r")) {
                        long lastPos = 0;
                        do {
                            randomAccessFile.seek(lastPos);
                            randomAccessFile.read(bytes);
                            Position position = Position.fromBytes(bytes);
                            lastPos = position.end;
                            if (!position.isDelete) {
                                deleteFlag = false;
                                break;
                            }
                        } while (lastPos < fileLength);
                    } catch (IOException e) {
                        deleteFlag = false;
                        LOGGER.error("FileCleaner collect fail!", e);
                    } finally {
                        if (deleteFlag) {
                            if (file.delete()) {
                                LOGGER.info("file {} is useless! delete successful!", file.getName());
                            } else {
                                LOGGER.warn("file {} is useless! but delete fail!", file.getName());
                            }
                        }
                    }
                }
            }
        }

        public void start() {
            timer.schedule(this, interval, interval);
        }

        public void shutdown() {
            timer.cancel();
        }

        public void setInterval(long interval) {
            this.interval = interval;
        }
    }
}
