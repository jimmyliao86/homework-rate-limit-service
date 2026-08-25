package com.example.demo.service;

import org.springframework.beans.factory.NoSuchBeanDefinitionException;
import org.springframework.beans.factory.ObjectProvider;

import com.example.demo.mq.RateLimitEventPublisher;

/**
 * A present-or-absent {@link ObjectProvider}, standing in for the container's own.
 *
 * <p>Written by hand rather than mocked deliberately. {@code ifAvailable} is a default
 * method on the interface, and a Mockito mock replaces it with a no-op -- so "no publisher
 * means nothing is published" would pass against a service that had no such branch at all.
 * Implementing {@code getIfAvailable} and letting the real default method run is what makes
 * both halves of that assertion mean something.
 */
class TestObjectProvider<T> implements ObjectProvider<T> {

    private final T instance;

    private TestObjectProvider(T instance) {
        this.instance = instance;
    }

    static ObjectProvider<RateLimitEventPublisher> of(RateLimitEventPublisher publisher) {
        return new TestObjectProvider<>(publisher);
    }

    /** What {@code rocketmq.enabled=false} looks like to a service. */
    static ObjectProvider<RateLimitEventPublisher> empty() {
        return new TestObjectProvider<>(null);
    }

    @Override
    public T getObject() {
        if (instance == null) {
            throw new NoSuchBeanDefinitionException("no instance available");
        }
        return instance;
    }

    @Override
    public T getObject(Object... args) {
        return getObject();
    }

    @Override
    public T getIfAvailable() {
        return instance;
    }

    @Override
    public T getIfUnique() {
        return instance;
    }
}
