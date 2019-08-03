package com.wanshifu.transformers.common.utils.queue;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

/**
 * 这个东西性能不算很好，和带锁环形队列吞吐量差不多
 *
 * @param <T>
 */
public class LinkedRingQueue<T> implements RingQueue<T> {

    private final AtomicReference<Node<T>> currentNode = new AtomicReference<>();

    private Node<T> first;

    private int size;

    @SafeVarargs
    public LinkedRingQueue(T... t) {
        Objects.requireNonNull(t);
        if (t.length == 0) {
            throw new IllegalArgumentException("collection is empty!");
        }
        this.init(Arrays.asList(t));
    }

    public LinkedRingQueue(Collection<T> collection) {
        if (collection == null) {
            throw new NullPointerException();
        }
        if (collection.size() == 0) {
            throw new IllegalArgumentException();
        }
        this.init(collection);
    }

    private void init(Collection<T> collection) {
        this.size = collection.size();
        Node<T> cursor = null;
        for (T t : collection) {
            if (cursor == null) {
                first = cursor = new Node<>(t);
            } else {
                Node<T> node = new Node<>(t);
                cursor = cursor.next = node;
            }
        }
        assert cursor != null;
        cursor.next = first;
        currentNode.set(first);
    }

    public T getAndMove() {
        for (; ; ) {
            Node<T> node = currentNode.get();
            if (currentNode.compareAndSet(node, node.next)) {
                return node.element;
            }
        }
    }

    @Override
    public List<T> getAll() {
        T first = getAndMove();
        T tmp;
        List<T> list = new ArrayList<>();
        while ((tmp = getAndMove()) != first) {
            list.add(tmp);
        }
        return list;
    }

    @Override
    public int size() {
        return size;
    }

    private static class Node<T> {

        private final T element;

        private Node<T> next;

        public Node(T element) {
            this.element = element;
        }
    }

}
