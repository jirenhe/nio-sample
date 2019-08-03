package com.wanshifu.transformers.common.remote.protocol;

import lombok.Data;
import lombok.Getter;

@Data
public class RpcResponse {

    private long timestamp;

    private String requestId;

    private String errorMsg;

    private Object result;

    public RpcResponse() {
    }

    public RpcResponse(String requestId, String errorMsg, Object result) {
        this.requestId = requestId;
        this.errorMsg = errorMsg;
        this.result = result;
        timestamp = System.currentTimeMillis();
    }

    public static RpcResponse error(String requestId, String errorMsg) {
        return new RpcResponse(requestId, errorMsg, null);
    }

    public static RpcResponse success(String requestId, Object result) {
        return new RpcResponse(requestId, null, result);
    }

}
