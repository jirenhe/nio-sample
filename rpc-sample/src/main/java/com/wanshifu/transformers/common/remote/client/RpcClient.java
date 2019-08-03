package com.wanshifu.transformers.common.remote.client;

import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;

public interface RpcClient {

    boolean isAlive();

    void connect() throws InterruptedException, Exception;

    void close();

    RpcResponse send(RpcRequest rpcRequest);
}
