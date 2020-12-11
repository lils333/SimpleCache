package com.lee.cache;

public class MemoryCache<K, V> extends EhcacheAny<K, V> {

    public MemoryCache(net.sf.ehcache.Cache cache, Class<K> keyType, Class<V> valueType) {
        super(cache, keyType, valueType);
    }
}
