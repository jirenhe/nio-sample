package com.wanshifu.transformers.core.channel.loadbalance;

import com.wanshifu.transformers.common.Context;
import com.wanshifu.transformers.common.UnexpectedException;
import com.wanshifu.transformers.common.constant.CoreConstants;
import com.wanshifu.transformers.common.utils.Configuration;
import com.wanshifu.transformers.common.utils.ConfigurationException;
import com.wanshifu.transformers.core.channel.worker.Worker;

import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LoadBalanceFactory {

    private final static Map<String, Class<? extends LoadBalance>> balanceMap = new HashMap<>();

    static {
        //吞吐量从高到底 ： RandomBalance > PollingBalance >= ColumnModBalance(需要看情况) > BiasedStarvingBalance
        //负载均衡效果：BiasedStarvingBalance >= PollingBalance > RandomBalance > ColumnModBalance
        balanceMap.put("random", RandomBalance.class);
        balanceMap.put("polling", PollingBalance.class);
        balanceMap.put("columnMod", ColumnModBalance.class);
        balanceMap.put("biasedStarving", BiasedStarvingBalance.class);
    }

    private LoadBalanceFactory() {
    }

    public static LoadBalance getLoadBalance(Context context, List<Worker> workers) throws ConfigurationException {
        Configuration configuration = context.getConfig().getConfiguration(CoreConstants.LOAD_BALANCE);
        LoadBalance loadBalance;
        if (configuration != null) {
            String type = configuration.getString(CoreConstants.TYPE);
            Class<? extends LoadBalance> clazz = balanceMap.get(type);
            if (clazz == null) {
                throw new ConfigurationException("balance type " + type + " is unrecognized!");
            }
            try {
                loadBalance = clazz.getConstructor(Context.class, List.class).newInstance(context, workers);
            } catch (InstantiationException | NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
                throw new UnexpectedException(e);
            }
        } else {
            loadBalance = new RandomBalance(context, workers);
        }
        return loadBalance;
    }
}
