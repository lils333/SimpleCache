package com.lee.cache.config;

public interface CacheConfiguration<K, V> {

    String name();

    String path();

    Class<K> key();

    Class<V> value();

    CacheType cache();

    enum Unit {
        /**
         * 指定存储在内存或者磁盘的空间大小
         */
        B(1), KB(1 << 10), MB(1 << 20), GB(1 << 30);

        private final int bytes;

        Unit(int bytes) {
            this.bytes = bytes;
        }

        public long toByte(long cacheSize) {
            return this.bytes * cacheSize;
        }

        public int toByte(int cacheSize) {
            return this.bytes * cacheSize;
        }
    }

    enum EvictionPolicy {
        /**
         * 指定驱逐的策略，也就是如果过期了，怎么驱逐
         */
        LRU, LFU, FIFO
    }
}
