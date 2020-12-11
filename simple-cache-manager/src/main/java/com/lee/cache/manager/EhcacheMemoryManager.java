package com.lee.cache.manager;

import com.lee.cache.Cache;
import com.lee.cache.Ehcache;
import com.lee.cache.EhcacheAny;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.EhcacheMemoryConfiguration;
import com.lee.cache.serializer.Serializer;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.config.PersistenceConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class EhcacheMemoryManager extends BaseCacheManager {

    private net.sf.ehcache.CacheManager cacheManager;

    @Override
    protected <K, V> Cache<K, V> createMemory(CacheConfiguration<K, V> configuration) {
        EhcacheMemoryConfiguration<K, V> config = (EhcacheMemoryConfiguration<K, V>) configuration;
        net.sf.ehcache.config.CacheConfiguration nonePersistent
                = new net.sf.ehcache.config.CacheConfiguration().name(configuration.name());

        if (config.getMaxBytesInMemory() != 0) {
            nonePersistent.maxBytesLocalHeap(config.getMaxBytesInMemory(), getUnit(config.getUnitInMemory()));
        } else {
            nonePersistent.maxEntriesLocalHeap(config.getMaxEntriesInMemory());
        }

        nonePersistent.memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.fromString(config.getEvictionPolicy().name()))
                .timeToIdleSeconds(config.getTimeToIdleSeconds())
                .timeToLiveSeconds(config.getTimeToLiveSeconds())
                .persistence(
                        new PersistenceConfiguration().strategy(PersistenceConfiguration.Strategy.NONE)
                );

        getCacheManager(configuration).addCache(new net.sf.ehcache.Cache(nonePersistent));

        if (config.isAutoDetect()) {
            return new Ehcache<>(getCacheManager(config).getCache(config.name()));
        } else {
            Serializer<K> key = config.getSerializerKey();
            Serializer<V> value = config.getSerializerValue();
            EhcacheAny<K, V> ehcache;
            if (key != null && value != null) {
                ehcache = new EhcacheAny<>(getCacheManager(config).getCache(config.name()), key, value);
            } else {
                if (value != null) {
                    ehcache = new EhcacheAny<>(getCacheManager(config).getCache(config.name()), config.key(), value);
                } else {
                    ehcache = new EhcacheAny<>(
                            getCacheManager(config).getCache(config.name()), config.key(), config.value()
                    );
                }
            }
            return ehcache;
        }
    }

    protected <K, V> CacheManager getCacheManager(CacheConfiguration<K, V> config) {
        if (cacheManager == null) {
            cacheManager = CacheManager.create(
                    new Configuration().diskStore(new DiskStoreConfiguration().path(config.path()))
            );
        }
        return cacheManager;
    }

    protected MemoryUnit getUnit(CacheConfiguration.Unit unit) {
        switch (unit) {
            case B:
                return MemoryUnit.forUnit('b');
            case KB:
                return MemoryUnit.forUnit('k');
            case MB:
                return MemoryUnit.forUnit('m');
            case GB:
                return MemoryUnit.forUnit('g');
            default:
                throw new IllegalArgumentException("Can not recognize this unit " + unit);
        }
    }
}
