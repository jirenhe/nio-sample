package com.wanshifu.transformers.core.channel;

import com.wanshifu.transformers.common.Container;
import com.wanshifu.transformers.common.bean.Task;

public interface Channel extends Container {

    void accept(Task task);
}
