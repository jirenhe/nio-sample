package com.wanshifu.transformers.common.utils.queue;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockedRingQueue<T> implements RingQueue<T> {

    private T[] tab;

    private int size;

    private int cursor = -1;

    private final Lock lock = new ReentrantLock(false);

    @SafeVarargs
    public LockedRingQueue(T... t) {
        Objects.requireNonNull(t);
        if (t.length == 0) {
            throw new IllegalArgumentException("collection is empty!");
        }
        this.tab = t;
        this.size = t.length;
    }

    @SuppressWarnings("unchecked")
    public LockedRingQueue(Collection<T> collection) {
        Objects.requireNonNull(collection);
        if (collection.size() == 0) {
            throw new IllegalArgumentException("collection is empty!");
        }
        this.tab = (T[]) collection.toArray();
        this.size = tab.length;
    }

    @Override
    public T getAndMove() {
        lock.lock();
        try {
            int index = ++cursor;
            if (index == Integer.MAX_VALUE) {
                cursor = 0;
            }
            return tab[index % size];
        } finally {
            lock.unlock();
        }
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
