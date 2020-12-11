package com.lee.cache.config;

public enum CacheType {
    /**
     * 缓存类，主要用于指定什么方式来作为缓存
     */
    EHCACHE,
    LEVELDBJNI,
    LEVELDBJNA,
    EHCACHE_MEMORY,
    ROCKSDB
}
