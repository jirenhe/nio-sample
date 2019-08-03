package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public class RpcServiceProxy implements InvocationHandler {

    private final Class<?> proxyClass;

    private final String address;

    private final int port;

    private final Serializer serializer;

    public RpcServiceProxy(Class<?> proxyClass, String address, int port, Serializer serializer) {
        this.proxyClass = proxyClass;
        this.address = address;
        this.port = port;
        this.serializer = serializer;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        Object[] parameters = args;
        RpcRequest rpcRequest = new RpcRequest(proxyClass.getName(), methodName, parameters, parameterTypes);
        return null;
    }
}
