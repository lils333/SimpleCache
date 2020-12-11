package com.lee.cache;

import java.io.Closeable;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

public interface Cache<K, V> extends Closeable {

    V get(K key);

    V put(K key, V value);

    V delete(K key);

    void put(Map<K, V> keyValues);

    void delete(Collection<K> keys);

    void consumeKey(Consumer<K> consumer);

    void consumeValue(Consumer<V> consumer);

    void consume(BiConsumer<K, V> consumer);
}
