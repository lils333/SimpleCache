package com.lee.cache.manager;

import com.lee.cache.Cache;
import com.lee.cache.config.CacheConfiguration;

import java.io.Closeable;

public interface CacheManager extends Closeable {

    <K, V> Cache<K, V> getCache(CacheConfiguration<K, V> configuration);

    static CacheManager newCacheManager() {
        return new ProxyCacheManager();
    }
}
