package com.wanshifu.transformers.common.remote.server;

public interface RpcServer {

    void start(final RequestProcessor requestProcessor) throws Exception;

    void shutdown();

}
