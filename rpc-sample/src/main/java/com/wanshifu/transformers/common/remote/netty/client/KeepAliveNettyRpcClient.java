package com.wanshifu.transformers.common.remote.netty.client;

import com.wanshifu.transformers.common.remote.RpcException;
import com.wanshifu.transformers.common.remote.client.RpcClient;
import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;
import com.wanshifu.transformers.common.remote.protocol.serialize.Serializer;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeepAliveNettyRpcClient extends NettyRpcClient implements RpcClient {

    private final Map<String, Signal> signalMap = new ConcurrentHashMap<>();

    private final OnConnectLostListener onConnectLostListener;

    public KeepAliveNettyRpcClient(String remoteAddress, int port, Serializer serializer, OnConnectLostListener onConnectLostListener) {
        this(remoteAddress, port, serializer, 2, onConnectLostListener);
    }

    public KeepAliveNettyRpcClient(String remoteAddress, int port, Serializer serializer, int timeout, OnConnectLostListener onConnectLostListener) {
        this(remoteAddress, port, serializer, timeout, 5, onConnectLostListener);
    }

    public KeepAliveNettyRpcClient(String remoteAddress, int port, Serializer serializer, int timeout, int maxWait, OnConnectLostListener onConnectLostListener) {
        super(remoteAddress, port, serializer, timeout, maxWait);
        this.onConnectLostListener = onConnectLostListener;
    }

    @Override
    protected Signal getSignal(String requestId) {
        return signalMap.remove(requestId);
    }

    @Override
    protected void connectLost() {
        this.onConnectLostListener.connectLost(this);
    }

    @Override
    public RpcResponse send(RpcRequest rpcRequest) throws RpcException {
        if (!this.isAlive()) {
            this.connect();
        }
        return super.send(rpcRequest);
    }

    @Override
    protected Signal createSignal(String requestId) {
        Signal signal = new Signal();
        signalMap.put(requestId, signal);
        return signal;
    }

    @FunctionalInterface
    public interface OnConnectLostListener {
        void connectLost(KeepAliveNettyRpcClient keepAliveNettyRpcClient);
    }

}
