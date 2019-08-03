package com.wanshifu.transformers.core.job.transport;

import com.wanshifu.transformers.common.bean.Task;

public interface Transport {

    void send(Task task);

    void done();
}
