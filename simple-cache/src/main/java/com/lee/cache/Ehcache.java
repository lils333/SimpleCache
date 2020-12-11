package com.lee.cache;

import net.sf.ehcache.Element;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

public class Ehcache<K, V> extends EhcacheAny<K, V> {

    private net.sf.ehcache.Cache cache;

    public Ehcache(net.sf.ehcache.Cache cache) {
        this.cache = cache;
    }

    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        Element element = cache.get(key);
        if (element != null) {
            return (V) element.getObjectValue();
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        cache.put(new Element(key, value));
        return null;
    }

    @Override
    public V delete(K key) {
        cache.remove(key);
        return null;
    }

    @Override
    public void put(Map<K, V> keyValues) {
        cache.putAll(
                keyValues.entrySet().stream().map(kvEntry -> new Element(
                        kvEntry.getKey(),
                        kvEntry.getValue()

                )).collect(Collectors.toList())
        );
    }

    @Override
    public void delete(Collection<K> keys) {
        cache.removeAll(keys);
    }

    @Override
    @SuppressWarnings("unchecked")
    public void consumeKey(Consumer<K> consumer) {
        List keys = cache.getKeys();
        if (keys != null) {
            for (Object key : keys) {
                consumer.accept((K) key);
            }
        }
    }

    @Override
    public void close() throws IOException {
        cache.flush();
        cache = null;
    }
}
