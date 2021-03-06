package com.lee.cache.config;

import com.lee.cache.serializer.Serializer;

public class LevelDbJnaConfiguration<K, V> extends Configuration<K, V> {

    private boolean isCreatedIfMissing = true;
    private long cacheSize = 64 * 1024 * 1024L;
    private int writeBufferSize = 16 * 1024 * 1024;
    private boolean verifyChecksums = true;
    private int maxOpenFiles = 2000;

    public LevelDbJnaConfiguration() {
        super();
    }

    public LevelDbJnaConfiguration(Class<K> keyType, Class<V> valueType) {
        super(keyType, valueType);
    }

    @Override
    public CacheType cache() {
        return CacheType.LEVELDBJNA;
    }

    @Override
    public LevelDbJnaConfiguration<K, V> name(String name) {
        super.name(name);
        return this;
    }

    @Override
    public LevelDbJnaConfiguration<K, V> path(String cachePath) {
        super.path(cachePath);
        return this;
    }

    @Override
    public LevelDbJnaConfiguration<K, V> truncate(boolean truncate) {
        super.truncate(truncate);
        return this;
    }

    @Override
    public LevelDbJnaConfiguration<K, V> defaultSerializer(Serializer<Object> defaultSerializer) {
        super.defaultSerializer(defaultSerializer);
        return this;
    }

    @Override
    public LevelDbJnaConfiguration<K, V> serializerValue(Serializer<V> serializerValue) {
        super.serializerValue(serializerValue);
        return this;
    }

    @Override
    public LevelDbJnaConfiguration<K, V> serializerKey(Serializer<K> serializerKey) {
        super.serializerKey(serializerKey);
        return this;
    }

    public LevelDbJnaConfiguration<K, V> createdIfMissing(boolean isCreatedIfMissing) {
        this.isCreatedIfMissing = isCreatedIfMissing;
        return this;
    }

    public LevelDbJnaConfiguration<K, V> cacheSize(long cacheSize, Unit unit) {
        this.cacheSize = unit.toByte(cacheSize);
        return this;
    }

    public LevelDbJnaConfiguration<K, V> maxOpenFiles(int maxOpenFiles) {
        this.maxOpenFiles = maxOpenFiles;
        return this;
    }

    public LevelDbJnaConfiguration<K, V> writeBufferSize(int writeBufferSize, Unit unit) {
        this.writeBufferSize = unit.toByte(writeBufferSize);
        return this;
    }

    public LevelDbJnaConfiguration<K, V> verifyChecksums(boolean verifyChecksums) {
        this.verifyChecksums = verifyChecksums;
        return this;
    }

    public boolean isCreatedIfMissing() {
        return isCreatedIfMissing;
    }

    public long getCacheSize() {
        return cacheSize;
    }

    public int getWriteBufferSize() {
        return writeBufferSize;
    }

    public boolean isVerifyChecksums() {
        return verifyChecksums;
    }

    public int getMaxOpenFiles() {
        return maxOpenFiles;
    }
}
