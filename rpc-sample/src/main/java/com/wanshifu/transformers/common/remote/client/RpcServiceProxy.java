package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

public abstract class RpcServiceProxy implements InvocationHandler {

    private final Class<?> proxyClass;

    protected final RpcClient rpcClient;

    protected RpcServiceProxy(Class<?> proxyClass, RpcClient rpcClient) {
        this.proxyClass = proxyClass;
        this.rpcClient = rpcClient;
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
        String methodName = method.getName();
        Class<?>[] parameterTypes = method.getParameterTypes();
        RpcRequest rpcRequest = new RpcRequest(proxyClass.getName(), methodName, args, parameterTypes);
        RpcResponse rpcResponse = this.doRpcInvoke(rpcRequest);
        if (rpcResponse.getErrorMsg() != null) {
            throw new RpcException("remote invoke fail! msg : " + rpcResponse.getErrorMsg());
        }
        return rpcResponse.getResult();
    }

    protected abstract RpcResponse doRpcInvoke(RpcRequest rpcRequest) throws RpcException;
}
