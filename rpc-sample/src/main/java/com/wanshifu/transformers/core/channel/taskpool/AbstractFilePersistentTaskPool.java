package com.wanshifu.transformers.core.channel.taskpool;

import com.wanshifu.transformers.GlobalConfig;
import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.common.bean.ChannelTask;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.common.utils.StringUtils;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.nio.file.Files;
import java.nio.file.NotDirectoryException;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;

public abstract class AbstractFilePersistentTaskPool extends AbstractTaskPool {

    protected static final Schema<ChannelTask> TASK_SCHEMA = RuntimeSchema.getSchema(ChannelTask.class);

    protected static final int MB = 1024 * 1024;

    protected static final String FILE_EXTENSION = ".persistent";

    protected static final String BASE_DIR = "persistent_file";

    protected File dir;

    protected String dirPath;

    protected AbstractFilePersistentTaskPool(Context context) {
        super(context);
    }

    @Override
    public void init() throws InitException {
        Context context = getContext();
        String path = context.getConfig().getString(CoreConstants.TASK_POOL_PARAMETER_PATH);
        if (StringUtils.isEmpty(path)) {
            path = GlobalConfig.getGlobalConfig().get(CoreConstants.CONFIG_CONFIG_PATH);
            if (StringUtils.isEmpty(path)) {
                path = System.getProperty("user.home");
            }
        }
        path = path + File.separator + BASE_DIR + File.separator + "channel_" + context.getName();
        File file = Paths.get(path).toFile();
        if (file.mkdirs()) {
            LOGGER.info("make dir for file persistent successful! path is " + path);
        }
        if (!file.isDirectory()) {
            throw new ConfigurationException(new NotDirectoryException(path));
        }
        dir = file;
        dirPath = path;
        postInit();
    }

    protected void writeFile(byte[] data, File file) throws TaskPersistentException {
        ByteChannel byteChannel = null;
        ByteBuffer byteBuffer = null;
        long time = System.currentTimeMillis();
        try {
            byteBuffer = ByteBuffer.wrap(data);
            /*byteBuffer = ByteBuffer.allocateDirect(objectBytes.length);
            byteBuffer.put(objectBytes);
            byteBuffer.flip();*/
            byteChannel = Files.newByteChannel(file.toPath(), StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            byteChannel.write(byteBuffer);
        } catch (IOException e) {
            throw new TaskPersistentException(e);
        } finally {
            if (byteChannel != null) {
                try {
                    byteChannel.close();
                } catch (IOException e) {
                    LOGGER.error("byteChannel close fail!", e);
                }
            }
            if (byteBuffer != null) {
                byteBuffer.clear();
            }
        }
        LOGGER.debug("file write take time : {}", System.currentTimeMillis() - time);
    }

    /*private void persistent(Collection<ChannelTask> tasks) throws TaskPersistentException {
        ArrayList<ChannelTask> target = new ArrayList<>(tasks);
        File file = fileRingQueue.getAndMove();
        try {
            byte[] objectBytes = toBytes(target);
            long time = System.currentTimeMillis();
            Files.write(file.toPath(), objectBytes, StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            LOGGER.debug("file write take time : {}", System.currentTimeMillis() - time);
        } catch (IOException e) {
            throw new TaskPersistentException(e);
        }
    }*/

    protected <T> byte[] toBytes(T obj, Schema<T> schema, int bufferSize) {
        LinkedBuffer buffer = LinkedBuffer.allocate(bufferSize);
        long time = System.currentTimeMillis();
        final byte[] protostuff;
        try {
            protostuff = ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } finally {
            buffer.clear();
        }
        LOGGER.debug("object serialized take time : {}", System.currentTimeMillis() - time);
        return protostuff;
    }

    protected <T> T getFromBytes(byte[] bytes, Schema<T> schema) {
        T t = schema.newMessage();
        ProtostuffIOUtil.mergeFrom(bytes, t, schema);
        return t;
    }

    protected <T> T readFromFile(File file, Schema<T> schema) throws Exception {
        T t = schema.newMessage();
        byte[] bytes = Files.readAllBytes(Paths.get(file.toURI()));
        ProtostuffIOUtil.mergeFrom(bytes, t, schema);
        return t;
    }

    protected abstract void postInit() throws InitException;
}
