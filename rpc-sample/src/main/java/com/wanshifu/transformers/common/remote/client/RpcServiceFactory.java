package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
import com.wanshifu.transformers.common.remote.protocol.serialize.impl.ProtostuffSerializer;

import java.lang.reflect.Proxy;

public class RpcServiceFactory {

    public static <T> T getRemoteService(Class<T> serviceClass, String address, int port) throws RpcException {
        return getRemoteService(serviceClass, address, port, new ProtostuffSerializer());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getRemoteService(Class<T> serviceClass, String address, int port, Serializer serializer) throws RpcException {
        if (!serviceClass.isInterface()) {
            throw new RpcException(String.format("this class : %s is not a interface", serviceClass.getName()));
        }
        return (T) Proxy.newProxyInstance(RpcServiceFactory.class.getClassLoader(), new Class[]{serviceClass}, new RpcServiceProxy(serviceClass, address, port, serializer));
    }
}
