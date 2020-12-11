package com.lee.cache.config;

import com.lee.cache.serializer.Serializer;

/**
 * @author l46li
 */
public class EhcacheMemoryConfiguration<K, V> extends Configuration<K, V> {

    private int maxEntriesInMemory;
    private long maxBytesInMemory;
    private Unit unitInMemory;
    private long timeToLiveSeconds;
    private long timeToIdleSeconds;
    private EvictionPolicy evictionPolicy;

    public EhcacheMemoryConfiguration() {
        super();
    }

    @Override
    public CacheType cache() {
        return CacheType.EHCACHE_MEMORY;
    }

    @Override
    public EhcacheMemoryConfiguration<K, V> name(String name) {
        super.name(name);
        return this;
    }

    @Override
    public EhcacheMemoryConfiguration<K, V> path(String cachePath) {
        super.path(cachePath);
        return this;
    }

    public EhcacheMemoryConfiguration(Class<K> keyType, Class<V> valueType) {
        super(keyType, valueType);
    }

    public EhcacheMemoryConfiguration<K, V> timeToIdleSeconds(long timeToIdleSeconds) {
        this.timeToIdleSeconds = timeToIdleSeconds;
        return this;
    }

    public EhcacheMemoryConfiguration<K, V> timeToLiveSeconds(long timeToLiveSeconds) {
        this.timeToLiveSeconds = timeToLiveSeconds;
        return this;
    }

    public EhcacheMemoryConfiguration<K, V> maxEntriesInMemory(int maxEntriesInMemory) {
        if (this.maxBytesInMemory != 0) {
            throw new IllegalArgumentException("maxBytesInMemory has been used");
        }
        this.maxEntriesInMemory = maxEntriesInMemory;
        return this;
    }

    public EhcacheMemoryConfiguration<K, V> maxBytesInMemory(long maxBytesInMemory, Unit unit) {
        if (maxEntriesInMemory != 0) {
            throw new IllegalArgumentException("maxEntriesInMemory has been used");
        }
        this.maxBytesInMemory = maxBytesInMemory;
//        this.unitInMemory = getUnit(unit);
        this.unitInMemory = unit;
        return this;
    }

    public EhcacheMemoryConfiguration<K, V> evictionPolicy(EvictionPolicy evictionPolicy) {
        this.evictionPolicy = evictionPolicy;
        return this;
    }

    @Override
    public Configuration<K, V> defaultSerializer(Serializer<Object> defaultSerializer) {
        super.defaultSerializer(defaultSerializer);
        return this;
    }

    public int getMaxEntriesInMemory() {
        return maxEntriesInMemory;
    }

    public long getMaxBytesInMemory() {
        return maxBytesInMemory;
    }

    public Unit getUnitInMemory() {
        return unitInMemory;
    }

    public long getTimeToLiveSeconds() {
        return timeToLiveSeconds;
    }

    public long getTimeToIdleSeconds() {
        return timeToIdleSeconds;
    }

    public EvictionPolicy getEvictionPolicy() {
        return evictionPolicy;
    }

//    protected MemoryUnit getUnit(Unit unit) {
//        switch (unit) {
//            case B:
//                return MemoryUnit.forUnit('b');
//            case KB:
//                return MemoryUnit.forUnit('k');
//            case MB:
//                return MemoryUnit.forUnit('m');
//            case GB:
//                return MemoryUnit.forUnit('g');
//            default:
//                throw new IllegalArgumentException("Can not recognize this unit " + unit);
//        }
//    }
}
