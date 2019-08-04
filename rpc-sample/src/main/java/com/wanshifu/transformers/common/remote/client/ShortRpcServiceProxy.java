package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;

public class ShortRpcServiceProxy extends RpcServiceProxy {

    private final RpcClient rpcClient;

    protected ShortRpcServiceProxy(Class<?> proxyClass, RpcClient rpcClient) {
        super(proxyClass);
        this.rpcClient = rpcClient;
    }

    @Override
    protected RpcResponse doRpcInvoke(RpcRequest rpcRequest) throws RpcException {
        synchronized (this) {
            rpcClient.connect();
            RpcResponse rpcResponse = rpcClient.send(rpcRequest);
            rpcClient.close();
            return rpcResponse;
        }
    }
}
