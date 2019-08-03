package com.wanshifu.transformers.common.remote.server;

import com.wanshifu.transformers.common.remote.protocol.RpcRequest;
import com.wanshifu.transformers.common.remote.protocol.RpcResponse;

public interface RequestProcessor {

    RpcResponse action(RpcRequest rpcRequest);
}
