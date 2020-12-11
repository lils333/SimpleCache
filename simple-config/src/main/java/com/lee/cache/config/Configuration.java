package com.lee.cache.config;

import com.lee.cache.serializer.Serializer;
import com.lee.cache.serializer.fst.FstJdkSerializer;

/**
 * @author l46li
 */
public abstract class Configuration<K, V> implements CacheConfiguration<K, V> {

    private Class<K> keyType;
    private Class<V> valueType;

    private boolean truncate = false;
    private boolean isAutoDetect = false;
    private String cachePath;
    private String name = "__default__cache__";

    private Serializer<K> serializerKey;
    private Serializer<V> serializerValue;

    /**
     * 该序列化器，可以序列化任何对象，只要实现了java的序列化接口
     * 如果想要序列化瞬态对象，那么需要在需要序列化的对象里面添加readObject和writeObject来显示的指定序列化过程
     */
    private Serializer<Object> defaultSerializer;

    public Configuration() {
        this.isAutoDetect = true;
        this.defaultSerializer = new FstJdkSerializer();
    }

    public Configuration(Class<K> keyType, Class<V> valueType) {
        this.keyType = keyType;
        this.valueType = valueType;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String path() {
        return this.cachePath;
    }

    public Configuration<K, V> name(String name) {
        this.name = name;
        return this;
    }

    public Configuration<K, V> path(String cachePath) {
        this.cachePath = cachePath;
        return this;
    }

    public Configuration<K, V> truncate(boolean truncate) {
        this.truncate = truncate;
        return this;
    }

    public Configuration<K, V> defaultSerializer(Serializer<Object> defaultSerializer) {
        this.defaultSerializer = defaultSerializer;
        return this;
    }

    /**
     * 默认如果不指定的话，那么使用ProtoStuffSerializer去运行时创建，不过会缓存的，所以性能没有问题
     *
     * @param serializerKey 序列化key
     * @return this
     */
    public Configuration<K, V> serializerKey(Serializer<K> serializerKey) {
        this.isAutoDetect = false;
        this.serializerKey = serializerKey;
        return this;
    }

    /**
     * 默认如果不指定的话，那么使用ProtoStuffSerializer去运行时创建，不过会缓存的，所以性能没有问题
     *
     * @param serializerValue 序列化value
     * @return this
     */
    public Configuration<K, V> serializerValue(Serializer<V> serializerValue) {
        this.isAutoDetect = false;
        this.serializerValue = serializerValue;
        return this;
    }

    @Override
    public Class<K> key() {
        return keyType;
    }

    @Override
    public Class<V> value() {
        return valueType;
    }

    public Serializer<K> getSerializerKey() {
        return serializerKey;
    }

    public Serializer<V> getSerializerValue() {
        return serializerValue;
    }

    public boolean isTruncate() {
        return truncate;
    }

    public boolean isAutoDetect() {
        return isAutoDetect;
    }

    public Serializer<Object> getDefaultSerializer() {
        return defaultSerializer;
    }
}
