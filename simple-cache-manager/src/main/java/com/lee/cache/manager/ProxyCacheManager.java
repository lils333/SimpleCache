package com.lee.cache.manager;

import com.lee.cache.Cache;
import com.lee.cache.config.CacheConfiguration;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * 它是一个代理的CacheManager，它会把具体的cache获取逻辑派发到被代理对象里面去,它主要的作用就是根据cache的类型来分类
 * 每一个类型用同一个CacheManager
 */
@Slf4j
public class ProxyCacheManager implements CacheManager {

    /**
     * 按照cache的类型来，每一个类型一个CacheManager
     */
    private final Map<String, CacheManager> cacheManagers = new HashMap<>();

    @Override
    public synchronized <K, V> Cache<K, V> getCache(CacheConfiguration<K, V> config) {
        CacheManager cacheManager;
        String name = config.cache().name();
        if ((cacheManager = cacheManagers.get(name)) == null) {
            switch (config.cache()) {
                case LEVELDBJNI:
                    cacheManager = new LevelDbJniManager();
                    break;
                case LEVELDBJNA:
                    cacheManager = new LevelDbJnaManager();
                    break;
                case ROCKSDB:
                    cacheManager = new RocksDbManager();
                    break;
                case EHCACHE:
                    cacheManager = new EhcacheManager();
                    break;
                case EHCACHE_MEMORY:
                default:
                    cacheManager = new EhcacheMemoryManager();
            }
            cacheManagers.put(name, cacheManager);
        }
        return cacheManager.getCache(config);
    }

    @Override
    public void close() {
        for (Map.Entry<String, CacheManager> entry : cacheManagers.entrySet()) {
            try {
                CacheManager cacheManager = entry.getValue();
                cacheManager.close();
            } catch (IOException e) {
                log.error("Can not close CacheManager : " + entry.getKey(), e);
            }
        }
        cacheManagers.clear();
    }
}
