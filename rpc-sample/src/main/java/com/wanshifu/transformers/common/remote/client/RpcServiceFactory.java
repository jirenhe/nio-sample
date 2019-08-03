package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.netty.client.KeepAliveNettyRpcClient;
import com.wanshifu.transformers.common.remote.netty.client.OnceNettyRpcClient;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;
import com.wanshifu.transformers.common.remote.protocol.serialize.impl.ProtostuffSerializer;

import java.lang.reflect.Proxy;
import java.util.HashMap;
import java.util.Map;

public class RpcServiceFactory {

    private static Map<String, RpcClient> RPC_CLIENT_MAP = new HashMap<>();

    public static <T> T getLongKeepRemoteService(Class<T> serviceClass, String address, int port) throws Exception {
        return getLongKeepRemoteService(serviceClass, address, port, new ProtostuffSerializer());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getLongKeepRemoteService(Class<T> serviceClass, String address, int port, Serializer serializer) throws Exception {
        if (!serviceClass.isInterface()) {
            throw new RpcException(String.format("this class : %s is not a interface", serviceClass.getName()));
        }
        String key = address + port;
        RpcClient rpcClient = RPC_CLIENT_MAP.get(key);
        if (rpcClient == null || !rpcClient.isAlive()) {
            synchronized (RpcServiceFactory.class) {
                rpcClient = RPC_CLIENT_MAP.get(key);
                if (rpcClient == null || !rpcClient.isAlive()) {
                    rpcClient = new KeepAliveNettyRpcClient(address, port, serializer, 5, 5, keepAliveNettyRpcClient -> RPC_CLIENT_MAP.remove(key));
                    rpcClient.connect();
                    RPC_CLIENT_MAP.put(key, rpcClient);
                }
            }
        }
        return (T) Proxy.newProxyInstance(RpcServiceFactory.class.getClassLoader(), new Class[]{serviceClass},
                new RpcServiceProxy(serviceClass, rpcClient));
    }


    public static <T> T getOnceRemoteService(Class<T> serviceClass, String address, int port) throws Exception {
        return getOnceRemoteService(serviceClass, address, port, new ProtostuffSerializer());
    }

    @SuppressWarnings("unchecked")
    public static <T> T getOnceRemoteService(Class<T> serviceClass, String address, int port, Serializer serializer) throws Exception {
        if (!serviceClass.isInterface()) {
            throw new RpcException(String.format("this class : %s is not a interface", serviceClass.getName()));
        }
        return (T) Proxy.newProxyInstance(RpcServiceFactory.class.getClassLoader(), new Class[]{serviceClass},
                new RpcServiceProxy(serviceClass, new OnceNettyRpcClient(address, port, serializer)));
    }
}
