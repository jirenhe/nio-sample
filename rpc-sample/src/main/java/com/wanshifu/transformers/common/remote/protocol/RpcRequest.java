package com.wanshifu.transformers.common.remote.protocol;

import lombok.Data;

import java.util.UUID;

@Data
public class RpcRequest {

    private String requestClass;

    private String requestMethod;

    private Object[] parameters;

    private Class<?>[] parameterTypes;

    private long timestamp;

    private String requestId;

    public RpcRequest() {
    }

    public RpcRequest(String requestClass, String requestMethod, Object[] parameters, Class<?>[] parameterTypes, String requestId) {
        this.requestClass = requestClass;
        this.requestMethod = requestMethod;
        this.parameters = parameters;
        this.parameterTypes = parameterTypes;
        this.requestId = requestId;
        this.timestamp = System.currentTimeMillis();
    }

    public RpcRequest(String requestClass, String requestMethod, Object[] parameters, Class<?>[] parameterTypes) {
        this(requestClass, requestMethod, parameters, parameterTypes, UUID.randomUUID().toString());
    }

}
