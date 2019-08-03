package com.wanshifu.transformers.common;

public abstract class ContextAware {

    private Context context;

    public void setContext(Context context) {
        this.context = context;
    }

    public Context getContext() {
        return this.context;
    }
}
