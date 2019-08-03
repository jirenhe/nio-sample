package com.wanshifu.transformers.common.remote.server;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.netty.NettyRpcServer;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
import com.wanshifu.transformers.common.remote.protocol.serialize.impl.ProtostuffSerializer;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

public class ServerProvider implements RequestProcessor {

    private final RpcServer rpcServer;

    private final Map<Class<?>, Object> servicesMapping = new HashMap<>();

    public ServerProvider(String localAddress, int port) {
        rpcServer = new NettyRpcServer(localAddress, port, new ProtostuffSerializer());
    }

    public ServerProvider(String localAddress, int port, Serializer serializer) {
        rpcServer = new NettyRpcServer(localAddress, port, serializer);
    }

    public void registerService(Class<?> serviceClass, Object imp) {
        servicesMapping.put(serviceClass, imp);
    }

    public void start() throws Exception {
        rpcServer.start(this);
    }

    public void shutdown() {
        rpcServer.shutdown();
    }

    @Override
    public RpcResponse action(RpcRequest rpcRequest) {
        String requestId = rpcRequest.getRequestId();
        Class<?> serviceClass = null;
        try {
            serviceClass = Class.forName(rpcRequest.getRequestClass());
        } catch (ClassNotFoundException e) {
            //todo logger this
        }
        Object service = servicesMapping.get(serviceClass);
        if (service != null) {
            Object[] parameters = rpcRequest.getParameters();
            Class<?>[] parameterTypes = rpcRequest.getParameterTypes();
            String method = rpcRequest.getRequestMethod();
            try {
                Object result = invoke(service, parameters, parameterTypes, method);
                return RpcResponse.success(requestId, result);
            } catch (RpcException e) {
                return RpcResponse.error(requestId, e.getMessage());
            } catch (Throwable e) {
                //todo get exception stack
                return RpcResponse.error(requestId, e.getMessage());
            }
        } else {
            return RpcResponse.error(requestId, String.format("service : %s is not register! please check it!", rpcRequest.getRequestClass()));
        }
    }


    private Object invoke(Object service, Object[] parameters, Class<?>[] parameterTypes, String methodName) throws RpcException {
        Class<?> serviceClass = service.getClass();
        try {
            Method method = serviceClass.getMethod(methodName, parameterTypes);
            return method.invoke(service, parameters);
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new RpcException(String.format("service : %s method is not exist! please check it!", methodName));
        } catch (InvocationTargetException e) {
            //todo get exception stack
            throw new RpcException("rpc error!");
        }
    }

}
