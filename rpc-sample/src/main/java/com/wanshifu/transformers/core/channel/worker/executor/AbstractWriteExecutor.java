package com.wanshifu.transformers.core.channel.worker.executor;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.ContextAware;
import com.wanshifu.transformers.plugin.write.Writer;

public abstract class AbstractWriteExecutor extends ContextAware implements WriteExecutor {

    protected final int id;

    protected final Writer writer;

    public AbstractWriteExecutor(Context context, int id, Writer writer) {
        this.setContext(context);
        this.id = id;
        this.writer = writer;
    }
}
