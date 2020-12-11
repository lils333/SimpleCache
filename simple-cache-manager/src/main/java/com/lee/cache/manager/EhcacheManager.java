package com.lee.cache.manager;

import com.lee.cache.Ehcache;
import com.lee.cache.EhcacheAny;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.EhcacheConfiguration;
import com.lee.cache.serializer.Serializer;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.config.Configuration;
import net.sf.ehcache.config.DiskStoreConfiguration;
import net.sf.ehcache.config.MemoryUnit;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

public class EhcacheManager extends BaseCacheManager {

    private net.sf.ehcache.CacheManager cacheManager;

    @Override
    protected <K, V> EhcacheAny<K, V> createEhcache(CacheConfiguration<K, V> configuration) {
        EhcacheConfiguration<K, V> config = (EhcacheConfiguration<K, V>) configuration;

        getCacheManager(config).addCache(
                new net.sf.ehcache.Cache(getCacheConfiguration(config))
        );

        if (config.isAutoDetect()) {
            return new Ehcache<>(getCacheManager(config).getCache(configuration.name()));
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

    protected <K, V> net.sf.ehcache.config.CacheConfiguration getCacheConfiguration(EhcacheConfiguration<K, V> config) {
        net.sf.ehcache.config.CacheConfiguration persistence
                = new net.sf.ehcache.config.CacheConfiguration().name(config.name());

        if (config.getMaxBytesInMemory() != 0) {
            persistence.maxBytesLocalHeap(config.getMaxBytesLocalDisk(), getUnit(config.getUnitInMemory()));
        } else if (config.getMaxEntriesInMemory() != 0) {
            persistence.maxEntriesLocalHeap(config.getMaxEntriesInMemory());
        } else {
            throw new IllegalArgumentException("maxBytesInMemory or maxEntriesInMemory must set");
        }

        persistence.maxBytesLocalDisk(config.getMaxBytesLocalDisk(), getUnit(config.getUnitInDisk()))
                .memoryStoreEvictionPolicy(MemoryStoreEvictionPolicy.fromString(config.getEvictionPolicy().name()))
                .timeToLiveSeconds(config.getTimeToLiveSeconds())
                .timeToIdleSeconds(config.getTimeToIdleSeconds())
                .overflowToDisk(true)
                .diskPersistent(true);
        return persistence;
    }

    protected <K, V> CacheManager getCacheManager(CacheConfiguration<K, V> config) {
        if (cacheManager == null) {
            cacheManager = CacheManager.create(
                    new Configuration().diskStore(new DiskStoreConfiguration().path(config.path()))
            );
        }
        return cacheManager;
    }

    @Override
    public void close() {
        super.close();
        cacheManager.shutdown();
        cacheManager = null;
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
