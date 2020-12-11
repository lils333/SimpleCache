package com.lee.cache.manager;

import com.lee.cache.BaseCache;
import com.lee.cache.Cache;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.Configuration;
import com.lee.cache.exception.CacheException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Slf4j
abstract class BaseCacheManager implements CacheManager {

    private final Object lock = new Object();
    private final Map<String, Cache> caches;

    private volatile boolean isClose;

    public BaseCacheManager() {
        this.caches = new HashMap<>();
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> Cache<K, V> getCache(CacheConfiguration<K, V> configuration) {
        if (StringUtils.isBlank(configuration.path())) {
            throw new IllegalArgumentException("Cache persistence path cannot be null or empty.");
        }

        String name = configuration.name();

        if (StringUtils.isBlank(name) || isClose) {
            throw new IllegalArgumentException("CacheManager is close or Cache name cannot be null or empty.");
        }
        Cache<K, V> cache;
        cache = caches.get(name);
        if (cache == null) {
            synchronized (lock) {
                if ((cache = caches.get(name)) != null) {
                    return cache;
                }
                caches.put(name, cache = createCache((Configuration<K, V>) configuration));
            }
        } else {
            validateCache((BaseCache<K, V>) cache, (Configuration<K, V>) configuration);
        }
        return cache;
    }

    @Override
    public void close() {
        isClose = true;
        for (Map.Entry<String, Cache> entry : caches.entrySet()) {
            Cache cache = entry.getValue();
            try {
                cache.close();
            } catch (IOException e) {
                log.error("Can not close cache " + entry.getKey(), e);
            }
        }
        caches.clear();
    }

    private <K, V> Cache<K, V> createCache(CacheConfiguration<K, V> configuration) {
        prepareEnv(configuration);
        switch (configuration.cache()) {
            case LEVELDBJNI:
                return createLevelDbJni(configuration);
            case LEVELDBJNA:
                return createLevelDbJna(configuration);
            case ROCKSDB:
                return createRocksDb(configuration);
            case EHCACHE:
                return createEhcache(configuration);
            case EHCACHE_MEMORY:
            default:
                return createMemory(configuration);
        }
    }

    //如果没有显示指定序列化器，那么默认就是可以序列化任何java对象
    private <V, K> void validateCache(BaseCache<K, V> cache, Configuration<K, V> config) {
        if (!cache.isSerializeAny()) {
            if (config.isAutoDetect()) {
                throw new CacheException(
                        "Cache exists with serialize key " + cache.getSerializerKey() +
                                " serialize value " + cache.getSerializerValue() +
                                ", can not use default serializer " + config.getDefaultSerializer()
                );
            } else {
                if (cache.getSerializerKey().getType().isAssignableFrom(config.key())
                        && cache.getSerializerValue().getType().isAssignableFrom(config.value())) {
                    log.info("Cache exists with serialize key " + cache.getSerializerKey() +
                            " serialize value " + cache.getSerializerValue() +
                            ", return current cache"
                    );
                } else {
                    throw new CacheException(
                            "Cache exists with serialize key " + cache.getSerializerKey() +
                                    " serialize value " + cache.getSerializerValue() +
                                    ", can not use this cache to serialize key " + config.key() +
                                    " and value " + config.value()
                    );
                }
            }
        } else {
            //默认现有的序列化器可以序列化任何java对象，实现了序列化接口，那么该Cache就可以存储任何实现了序列化接口的对象
            //实际底层是基于字节数组来存储的，所以这个主要是看各个序列化器
            log.info("Cache use default serializer {} to serialize any java Object ", cache.getDefaultSerializer());
        }
    }

    protected <K, V> Cache<K, V> createMemory(CacheConfiguration<K, V> configuration) {
        return null;
    }

    protected <K, V> Cache<K, V> createEhcache(CacheConfiguration<K, V> configuration) {
        return null;
    }

    protected <K, V> Cache<K, V> createLevelDbJni(CacheConfiguration<K, V> configuration) {
        return null;
    }

    protected <K, V> Cache<K, V> createLevelDbJna(CacheConfiguration<K, V> configuration) {
        return null;
    }

    protected <K, V> Cache<K, V> createRocksDb(CacheConfiguration<K, V> configuration) {
        return null;
    }

    private <K, V> void prepareEnv(CacheConfiguration<K, V> config) {
        try {
            FileUtils.forceMkdir(new File(config.path()));
        } catch (IOException e) {
            throw new CacheException("Can not create level db dir " + config.path(), e);
        }
    }
}
