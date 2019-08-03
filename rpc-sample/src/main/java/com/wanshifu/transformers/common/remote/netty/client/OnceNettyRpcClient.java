package com.wanshifu.transformers.common.remote.netty.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.client.RpcClient;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;

import java.util.concurrent.atomic.AtomicBoolean;

public class OnceNettyRpcClient extends NettyRpcClient implements RpcClient {

    private final Signal signal = new Signal();

    private final AtomicBoolean atomicBoolean = new AtomicBoolean(true);

    public OnceNettyRpcClient(String remoteAddress, int port, Serializer serializer) {
        this(remoteAddress, port, serializer, 2);
    }

    public OnceNettyRpcClient(String remoteAddress, int port, Serializer serializer, int timeout) {
        super(remoteAddress, port, serializer, timeout, 1);
    }

    @Override
    protected Signal getSignal(String requestId) {
        return signal;
    }

    @Override
    public RpcResponse send(RpcRequest rpcRequest) throws RpcException {
        boolean flag = atomicBoolean.compareAndSet(true, false);
        if (flag) {
            RpcResponse rpcResponse = super.send(rpcRequest);
            this.close();
            return rpcResponse;
        } else {
            throw new RpcException("this connect was used!");
        }
    }

    @Override
    protected void connectLost() {
    }

    @Override
    protected Signal createSignal(String requestId) {
        return signal;
    }

}
