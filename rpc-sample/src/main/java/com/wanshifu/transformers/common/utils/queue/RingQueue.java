package com.wanshifu.transformers.common.utils.queue;

import java.util.List;

public interface RingQueue<T> {

    T getAndMove();

    int size();

    List<T> getAll();

}
