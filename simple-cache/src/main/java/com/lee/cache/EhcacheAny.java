package com.lee.cache;

import com.lee.cache.serializer.Serializer;
import lombok.extern.slf4j.Slf4j;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.IOException;
import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Collectors;

@Slf4j
public class EhcacheAny<K, V> extends BaseCache<K, V> {

    private net.sf.ehcache.Cache cache;

    EhcacheAny() {
        super();
    }

    public EhcacheAny(Cache cache, Class<K> keyType, Class<V> valueType) {
        super(keyType, valueType);
        this.cache = cache;
    }

    public EhcacheAny(Cache cache, Serializer<K> serializerKey, Serializer<V> serializerValue) {
        super(serializerKey, serializerValue);
        this.cache = cache;
    }

    public EhcacheAny(Cache cache, Class<K> keyType, Serializer<V> serializerValue) {
        super(keyType, serializerValue);
        this.cache = cache;
    }

    @Override
    public V get(K key) {
        Element element = cache.get(new Wrapper(serializeKey(key)));
        if (element != null) {
            Wrapper wrapper = (Wrapper) element.getObjectValue();
            return deserializeValue(wrapper.getBytes());
        }
        return null;
    }

    @Override
    public V put(K key, V value) {
        cache.put(new Element(new Wrapper(serializeKey(key)), new Wrapper(serializeValue(value))));
        return null;
    }

    @Override
    public V delete(K key) {
        cache.remove(new Wrapper(serializeKey(key)));
        return null;
    }

    @Override
    public void put(Map<K, V> keyValues) {
        cache.putAll(
                keyValues.entrySet().stream().map(kvEntry -> new Element(
                        new Wrapper(serializeKey(kvEntry.getKey())),
                        new Wrapper(serializeValue(kvEntry.getValue()))

                )).collect(Collectors.toList())
        );
    }

    @Override
    public void delete(Collection<K> keys) {
        cache.removeAll(
                keys.stream().map(key -> new Wrapper(serializeKey(key))).collect(Collectors.toList())
        );
    }

    @Override
    public void consumeKey(Consumer<K> consumer) {
        List keys = cache.getKeys();
        if (keys != null) {
            for (Object key : keys) {
                Wrapper wrapper = (Wrapper) key;
                consumer.accept(deserializeKey(wrapper.getBytes()));
            }
        }
    }

    @Override
    public void close() throws IOException {
        cache.flush();
        cache = null;
    }

    /**
     * ehcache 内部使用了element去存储，而且要求存储的对象必须继承于Serializable，所以，在这个地方，我们封装了一下
     */
    private static class Wrapper implements Serializable {

        private final byte[] bytes;

        Wrapper(byte[] bytes) {
            this.bytes = bytes;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }

            if (o == null || getClass() != o.getClass()) {
                return false;
            }

            Wrapper wrapper = (Wrapper) o;

            return new EqualsBuilder()
                    .append(bytes, wrapper.bytes)
                    .isEquals();
        }

        @Override
        public int hashCode() {
            return new HashCodeBuilder(17, 37)
                    .append(bytes)
                    .toHashCode();
        }

        public byte[] getBytes() {
            return bytes;
        }
    }
}
