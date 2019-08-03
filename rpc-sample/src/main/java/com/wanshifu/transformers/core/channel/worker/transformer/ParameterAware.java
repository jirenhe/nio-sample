package com.wanshifu.transformers.core.channel.worker.transformer;

import com.alibaba.fastjson.JSONObject;

public interface ParameterAware {

    void setParameter(JSONObject parameter);
}
