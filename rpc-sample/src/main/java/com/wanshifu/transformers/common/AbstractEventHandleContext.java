package com.wanshifu.transformers.common;

import com.wanshifu.transformers.common.utils.Configuration;
import org.apache.commons.collections.CollectionUtils;

import java.util.*;

public abstract class AbstractEventHandleContext extends AbstractConfigAbleContext implements Context {

    private final Context parent;

    private final EventListenerRetriever eventListenerRetriever = new EventListenerRetriever();

    public AbstractEventHandleContext(Configuration configuration, Context parent) {
        super(configuration);
        this.parent = parent;
    }

    @Override
    public void registerEventListener(EventListener<?> eventListener) {
        eventListenerRetriever.addRegister(eventListener);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void publishEvent(Event event) {
        Objects.requireNonNull(event);
        List<EventListener> eventListeners = eventListenerRetriever.getRegister(event.getClass());
        for (EventListener eventListener : eventListeners) {
            eventListener.onEvent(event);
            if (null != parent) {
                parent.publishEvent(event);
            }
        }
    }

    protected static class EventListenerRetriever {

        private final Map<Class<?>, List<EventListener>> eventListenerMapCache = new HashMap<>();

        private final List<EventListener> nonGenericListenerList = new ArrayList<>();

        public EventListenerRetriever() {
        }

        private void putInCache(GenericListenerAdapter genericListenerAdapter) {
            Class<? extends Event> resolveType = genericListenerAdapter.getResolveType();
            if (resolveType != null) {
                List<EventListener> eventListeners = eventListenerMapCache.get(resolveType);
                if (CollectionUtils.isEmpty(eventListeners)) {
                    eventListeners = new ArrayList<>(nonGenericListenerList);
                    eventListenerMapCache.put(resolveType, eventListeners);
                }
                eventListeners.add(genericListenerAdapter);
            } else {
                eventListenerMapCache.forEach((aClass, eventListeners) -> eventListeners.add(genericListenerAdapter));
                nonGenericListenerList.add(genericListenerAdapter);
            }
        }

        public void addRegister(EventListener<?> eventListener) {
            this.putInCache(new GenericListenerAdapter(eventListener));
        }

        @SuppressWarnings("unchecked")
        public List<EventListener> getRegister(Class<?> aClass) {
            return Optional.ofNullable(eventListenerMapCache.get(aClass)).orElse(Collections.EMPTY_LIST);
        }
    }

}
