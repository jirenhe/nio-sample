package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.netty.client.NettyRpcClient;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
import com.wanshifu.transformers.common.remote.protocol.serialize.impl.ProtostuffSerializer;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

public class RpcServiceFactory {

    private static Map<String, RpcClient> RPC_CLIENT_MAP = new HashMap<>();

    public static <T> T getKeepAliveRemoteService(Class<T> serviceClass, String address, int port) throws Exception {
        return getKeepAliveRemoteService(serviceClass, address, port, 5000);
    }

    public static <T> T getKeepAliveRemoteService(Class<T> serviceClass, String address, int port, long timeout) throws Exception {
        return getKeepAliveRemoteService(serviceClass, address, port, timeout, 5);
    }

    public static <T> T getKeepAliveRemoteService(Class<T> serviceClass, String address, int port, long timeout, int maxWait) throws Exception {
        return getKeepAliveRemoteService(serviceClass, address, port, timeout, maxWait, new ProtostuffSerializer());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getKeepAliveRemoteService(Class<T> serviceClass, String address, int port, long timeout, int maxWait, Serializer serializer) throws Exception {
        if (!serviceClass.isInterface()) {
            throw new RpcException(String.format("this class : %s is not a interface", serviceClass.getName()));
        }
        String key = address + port;
        RpcClient rpcClient = RPC_CLIENT_MAP.get(key);
        if (rpcClient == null || !rpcClient.isAlive()) {
            synchronized (RpcServiceFactory.class) {
                rpcClient = RPC_CLIENT_MAP.get(key);
                if (rpcClient == null || !rpcClient.isAlive()) {
                    rpcClient = new NettyRpcClient(address, port, serializer, timeout, maxWait);
                    rpcClient.connect();
                    RPC_CLIENT_MAP.put(key, rpcClient);
                }
            }
        }
        return (T) Proxy.newProxyInstance(RpcServiceFactory.class.getClassLoader(), new Class[]{serviceClass},
                new KeepAliveRpcServiceProxy(serviceClass, rpcClient));
    }


    public static <T> T getShortConnectRemoteService(Class<T> serviceClass, String address, int port) throws Exception {
        return getShortConnectRemoteService(serviceClass, address, port, 5000);
    }

    public static <T> T getShortConnectRemoteService(Class<T> serviceClass, String address, int port, long timeout) throws Exception {
        return getShortConnectRemoteService(serviceClass, address, port, timeout, 5);
    }

    public static <T> T getShortConnectRemoteService(Class<T> serviceClass, String address, int port, long timeout, int maxWait) throws Exception {
        return getShortConnectRemoteService(serviceClass, address, port, timeout, maxWait, new ProtostuffSerializer());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getShortConnectRemoteService(Class<T> serviceClass, String address, int port, long timeout, int maxWait, Serializer serializer) throws Exception {
        if (!serviceClass.isInterface()) {
            throw new RpcException(String.format("this class : %s is not a interface", serviceClass.getName()));
        }
        return (T) Proxy.newProxyInstance(RpcServiceFactory.class.getClassLoader(), new Class[]{serviceClass},
                new ShortRpcServiceProxy(serviceClass, new NettyRpcClient(address, port, serializer, timeout, maxWait)));
    }
}
