package com.wanshifu.transformers;

import com.alibaba.fastjson.JSONObject;
import com.wanshifu.transformers.common.utils.Configuration;

public class GlobalConfig {

    private static Configuration GLOBAL_CONFIGURATION;

    private static JSONObject GLOBAL_CONFIGURATION_JSON;

    private GlobalConfig() {
    }

    public static Configuration getGlobalConfig() {
        return GLOBAL_CONFIGURATION;
    }

    public static JSONObject getGlobalConfigAsJson() {
        return GLOBAL_CONFIGURATION_JSON;
    }

    static void setConfig(Configuration configuration) {
        GLOBAL_CONFIGURATION = configuration;
        GLOBAL_CONFIGURATION_JSON = JSONObject.parseObject(configuration.toJSON());
    }
}
