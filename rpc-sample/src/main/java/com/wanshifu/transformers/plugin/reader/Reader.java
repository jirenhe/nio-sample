package com.wanshifu.transformers.plugin.reader;

import com.wanshifu.transformers.common.InitException;
import com.wanshifu.transformers.core.job.transport.Transport;

public interface Reader {

    void init() throws InitException;

    void startReader(Transport transport);

    void destroy();
}
