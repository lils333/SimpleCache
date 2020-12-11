package com.lee.cache.config;

import com.lee.cache.serializer.Serializer;

/**
 * @author l46li
 */
public class EhcacheConfiguration<K, V> extends EhcacheMemoryConfiguration<K, V> {

    private Unit unitInDisk;
    private long maxBytesLocalDisk;

    @Override
    public CacheType cache() {
        return CacheType.EHCACHE;
    }

    public EhcacheConfiguration() {
        super();
    }

    public EhcacheConfiguration(Class<K> keyType, Class<V> valueType) {
        super(keyType, valueType);
    }

    @Override
    public EhcacheConfiguration<K, V> timeToIdleSeconds(long timeToIdleSeconds) {
        super.timeToIdleSeconds(timeToIdleSeconds);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> truncate(boolean truncate) {
        super.truncate(truncate);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> timeToLiveSeconds(long timeToLiveSeconds) {
        super.timeToLiveSeconds(timeToLiveSeconds);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> maxEntriesInMemory(int maxEntriesInMemory) {
        super.maxEntriesInMemory(maxEntriesInMemory);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> maxBytesInMemory(long maxBytesInMemory, Unit unit) {
        super.maxBytesInMemory(maxBytesInMemory, unit);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> evictionPolicy(EvictionPolicy evictionPolicy) {
        super.evictionPolicy(evictionPolicy);
        return this;
    }

    public EhcacheConfiguration<K, V> maxBytesLocalDisk(int maxBytesLocalDisk, Unit unit) {
        this.maxBytesLocalDisk = maxBytesLocalDisk;
//        this.unitInDisk = getUnit(unit);
        this.unitInDisk = unit;
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> defaultSerializer(Serializer<Object> defaultSerializer) {
        super.defaultSerializer(defaultSerializer);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> serializerValue(Serializer<V> serializerValue) {
        super.serializerValue(serializerValue);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> name(String name) {
        super.name(name);
        return this;
    }

    @Override
    public EhcacheConfiguration<K, V> path(String cachePath) {
        super.path(cachePath);
        return this;
    }

    public Unit getUnitInDisk() {
        return unitInDisk;
    }

    public long getMaxBytesLocalDisk() {
        return maxBytesLocalDisk;
    }
}
