package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;

public class KeepAliveRpcServiceProxy extends RpcServiceProxy {

    private final RpcClient rpcClient;

    protected KeepAliveRpcServiceProxy(Class<?> proxyClass, RpcClient rpcClient) {
        super(proxyClass);
        this.rpcClient = rpcClient;
        Runtime.getRuntime().addShutdownHook(new Thread(rpcClient::close));
    }

    @Override
    protected RpcResponse doRpcInvoke(RpcRequest rpcRequest) throws RpcException {
        if (!rpcClient.isAlive()) {
            synchronized (this) {
                if(!rpcClient.isAlive()){
                    rpcClient.connect();
                }
            }
        }
        return rpcClient.send(rpcRequest);
    }
}
