package com.wanshifu.transformers.common.remote.protocol.serialize.impl;

import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import io.protostuff.runtime.RuntimeSchema;

import java.util.HashMap;
import java.util.Map;

@SuppressWarnings("unchecked")
public class ProtostuffSerializer implements Serializer {

    private static final Map<Class<?>, Schema<?>> CACHED_SCHEMA = new HashMap<>();

    @Override
    public <T> byte[] serialize(T obj) {
        Class<T> cls = (Class<T>) obj.getClass();
        LinkedBuffer buffer = LinkedBuffer.allocate(LinkedBuffer.DEFAULT_BUFFER_SIZE);
        try {
            Schema<T> schema = getSchema(cls);
            return ProtostuffIOUtil.toByteArray(obj, schema, buffer);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public <T> T deserialize(byte[] bytes, Class<T> clazz) throws Exception {
        T obj = clazz.newInstance();
        Schema<T> schema = getSchema(clazz);
        ProtostuffIOUtil.mergeFrom(bytes, obj, schema);
        return obj;
    }

    private <T> Schema<T> getSchema(Class<T> cls) {
        Schema<T> schema = (Schema<T>) CACHED_SCHEMA.get(cls);
        if (schema == null) {
            synchronized (CACHED_SCHEMA) {
                schema = (Schema<T>) CACHED_SCHEMA.get(cls);
                if (schema == null) {
                    schema = RuntimeSchema.createFrom(cls);
                    CACHED_SCHEMA.put(cls, schema);
                }
            }
        }
        return schema;
    }
}
