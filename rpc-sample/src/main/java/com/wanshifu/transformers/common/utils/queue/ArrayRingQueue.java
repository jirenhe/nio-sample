package com.wanshifu.transformers.common.utils.queue;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrayRingQueue<T> implements RingQueue<T> {

    private T[] tab;

    private int size;

    private final AtomicInteger cursor = new AtomicInteger(-1);

    @SafeVarargs
    public ArrayRingQueue(T... t) {
        Objects.requireNonNull(t);
        if (t.length == 0) {
            throw new IllegalArgumentException("collection is empty!");
        }
        this.tab = t;
        this.size = t.length;
    }

    @SuppressWarnings("unchecked")
    public ArrayRingQueue(Collection<T> collection) {
        if (collection == null) {
            throw new NullPointerException();
        }
        if (collection.size() == 0) {
            throw new IllegalArgumentException();
        }
        this.tab = (T[]) collection.toArray();
        this.size = tab.length;
    }

    @Override
    public T getAndMove() {
        int index = cursor.incrementAndGet();
        if (index == Integer.MAX_VALUE) {
            cursor.set(0);
        }
        return tab[index % size];
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public List<T> getAll() {
        return Arrays.asList(tab);
    }
}
