package com.lee.cache;

import com.lee.cache.serializer.DefaultSerializer;
import com.lee.cache.serializer.Serializer;
import com.lee.cache.serializer.protostuff.ProtoStuffSerializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ClassUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 包含序列化和反序列化方法，子类直接使用，而不需要在子类自己去实现
 * <p>
 * 如果子类需要一个资源监控器，配置DbBuffer一起完成资源长时间不用导致的资源泄露问题
 *
 * @param <K>
 * @param <V>
 */
@Slf4j
public abstract class BaseCache<K, V> implements Cache<K, V> {

    protected final V empty = null;

    private Serializer<Object> defaultSerializer;

    private Serializer<K> serializerKey;
    private Serializer<V> serializerValue;

    BaseCache() {
        super();
    }

    public BaseCache(Serializer<Object> defaultSerializer) {
        this.defaultSerializer = defaultSerializer;
    }

    @SuppressWarnings("unchecked")
    public BaseCache(Class<K> keyType, Class<V> valueType) {
        if (keyType == null || valueType == null) {
            throw new IllegalArgumentException("keyType or valueType must not be null");
        }

        if (ClassUtils.isPrimitiveOrWrapper(keyType)) {
            serializerKey = (Serializer<K>) getPrimitiveSerializer(keyType);
        } else {
            serializerKey = new ProtoStuffSerializer<>(keyType);
        }

        if (ClassUtils.isPrimitiveOrWrapper(valueType)) {
            serializerValue = (Serializer<V>) getPrimitiveSerializer(valueType);
        } else {
            serializerValue = new ProtoStuffSerializer<>(valueType);
        }
    }

    @SuppressWarnings("unchecked")
    public BaseCache(Class<K> keyType, Serializer<V> serializerValue) {
        if (keyType == null || serializerValue == null) {
            throw new IllegalArgumentException("keyType or serializerValue must not be null");
        }

        if (ClassUtils.isPrimitiveOrWrapper(keyType)) {
            serializerKey = (Serializer<K>) getPrimitiveSerializer(keyType);
        } else {
            serializerKey = new ProtoStuffSerializer<>(keyType);
        }

        this.serializerValue = serializerValue;
    }

    public BaseCache(Serializer<K> serializerKey, Serializer<V> serializerValue) {
        if (serializerKey == null || serializerValue == null) {
            throw new IllegalArgumentException("serializerKey or serializerValue must not be null");
        }
        this.serializerKey = serializerKey;
        this.serializerValue = serializerValue;
    }

    protected byte[] serializeAny(Object object) {
        return defaultSerializer.serialize(object);
    }

    protected Object deserializeAny(byte[] bytes) {
        return defaultSerializer.deserialize(bytes);
    }

    protected byte[] serializeKey(K key) {
        return serializerKey.serialize(key);
    }

    protected K deserializeKey(byte[] bytes) {
        return serializerKey.deserialize(bytes);
    }

    protected byte[] serializeValue(V key) {
        return serializerValue.serialize(key);
    }

    protected V deserializeValue(byte[] bytes) {
        return serializerValue.deserialize(bytes);
    }

    public Serializer<K> getSerializerKey() {
        return this.serializerKey;
    }

    public Serializer<V> getSerializerValue() {
        return this.serializerValue;
    }

    public Serializer<Object> getDefaultSerializer() {
        return this.defaultSerializer;
    }

    public boolean isSerializeAny() {
        return serializerKey == null && serializerValue == null;
    }

    @Override
    public void close() throws IOException {

    }

    @Override
    public void put(Map<K, V> keyValues) {
        throw new UnsupportedOperationException("UnsupportedOperationException : batch put ");
    }

    @Override
    public void delete(Collection<K> keys) {
        throw new UnsupportedOperationException("UnsupportedOperationException : batch delete ");
    }

    @Override
    public void consumeKey(Consumer<K> consumer) {
        throw new UnsupportedOperationException("UnsupportedOperationException : consumeKey ");
    }

    @Override
    public void consumeValue(Consumer<V> consumer) {
        throw new UnsupportedOperationException("UnsupportedOperationException : consumeValue ");
    }

    @Override
    public void consume(BiConsumer<K, V> consumer) {
        throw new UnsupportedOperationException("UnsupportedOperationException : consume ");
    }

    protected void closeQuit(AutoCloseable... closeables) {
        if (closeables != null) {
            for (AutoCloseable closeable : closeables) {
                try {
                    if (closeable != null) {
                        closeable.close();
                    }
                } catch (Exception e) {
                    log.warn("Can not close resource " + closeable, e);
                }
            }
        }
    }

    private Serializer<?> getPrimitiveSerializer(Class<?> keyType) {
        Class<?> primitive = ClassUtils.wrapperToPrimitive(keyType);
        switch (primitive.toGenericString()) {
            case "boolean":
                return DefaultSerializer.BOOLEAN_SERIALIZER;
            case "byte":
                return DefaultSerializer.BYTE_SERIALIZER;
            case "char":
                return DefaultSerializer.CHARACTER_SERIALIZER;
            case "short":
                return DefaultSerializer.SHORT_SERIALIZER;
            case "int":
                return DefaultSerializer.INTEGER_SERIALIZER;
            case "long":
                return DefaultSerializer.LONG_SERIALIZER;
            case "double":
                return DefaultSerializer.DOUBLE_SERIALIZER;
            case "float":
                return DefaultSerializer.FLOAT_SERIALIZER;
            default:
                throw new IllegalArgumentException("Can not recognize " + keyType);
        }
    }
}
