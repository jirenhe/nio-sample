package com.wanshifu.transformers.common;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Arrays;
import java.util.Objects;

public class GenericListenerAdapter implements EventListener<Event> {

    private final EventListener<Event> delegate;

    private final Class<? extends Event> resolveType;

    @SuppressWarnings("unchecked")
    public GenericListenerAdapter(EventListener<? extends Event> delegate) {
        this.delegate = (EventListener<Event>) delegate;
        this.resolveType = resolveDeclaredEventType(this.delegate);
    }

    @SuppressWarnings("unchecked")
    private Class<? extends Event> resolveDeclaredEventType(EventListener<?> delegate) {
        Objects.requireNonNull(delegate);
        Type[] tmp = delegate.getClass().getGenericInterfaces();
        Type listenerType = Arrays.stream(tmp)
                .filter(type -> {
                    if (type instanceof Class) {
                        return type.equals(EventListener.class);
                    }
                    if (type instanceof ParameterizedType) {
                        return ((ParameterizedType) type).getRawType().equals(EventListener.class);
                    }
                    return false;
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("this should never happened!"));
        if (listenerType instanceof ParameterizedType) {
            return (Class<? extends Event>) ((ParameterizedType) listenerType).getActualTypeArguments()[0];
        }
        return null;
    }

    @Override
    public void onEvent(Event event) {
        delegate.onEvent(event);
    }

    public EventListener<Event> getDelegate() {
        return delegate;
    }

    public Class<? extends Event> getResolveType() {
        return resolveType;
    }
}
