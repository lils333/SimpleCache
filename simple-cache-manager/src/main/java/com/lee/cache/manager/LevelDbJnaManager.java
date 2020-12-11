package com.lee.cache.manager;

import com.lee.cache.Cache;
import com.lee.cache.LevelDbJnaAnyCache;
import com.lee.cache.LevelDbJnaCache;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.LevelDbJnaConfiguration;
import com.lee.cache.serializer.Serializer;
import com.protonail.leveldb.jna.LevelDB;
import com.protonail.leveldb.jna.LevelDBOptions;
import lombok.extern.slf4j.Slf4j;

import java.io.File;

/**
 * @author l46li
 */
@Slf4j
public class LevelDbJnaManager extends BaseCacheManager {

    @Override
    protected <K, V> Cache<K, V> createLevelDbJna(CacheConfiguration<K, V> configuration) {
        LevelDbJnaConfiguration<K, V> config = (LevelDbJnaConfiguration<K, V>) configuration;

        if (config.isAutoDetect()) {
            return new LevelDbJnaAnyCache<>(createDb(config), config.getDefaultSerializer(),
                    config.isTruncate(),
                    config.path(),
                    configuration.name()
            );
        }

        Serializer<K> key = config.getSerializerKey();
        Serializer<V> value = config.getSerializerValue();

        LevelDbJnaCache<K, V> levelDbCache;
        if (key != null && value != null) {
            levelDbCache = new LevelDbJnaCache<>(createDb(config), key, value);
        } else {
            if (value != null) {
                levelDbCache = new LevelDbJnaCache<>(createDb(config), config.key(), value);
            } else {
                levelDbCache = new LevelDbJnaCache<>(createDb(config), config.key(), config.value());
            }
        }

        if (config.isTruncate()) {
            //如果truncate配置true，那么意思就是说需要要db名字为name的清空
            levelDbCache.truncate(config.isTruncate(), config.path(), config.name());
        }
        return levelDbCache;
    }

    private <K, V> LevelDB createDb(LevelDbJnaConfiguration<K, V> config) {
        try (LevelDBOptions options = new LevelDBOptions()) {
            options.setCreateIfMissing(true);
            options.setMaxOpenFiles(config.getMaxOpenFiles());
            options.setWriteBufferSize(config.getWriteBufferSize());
            //name才是真正的db的名字，而path决定db存储的位置
            return new LevelDB(config.path() + File.separator + config.name(), options);
        }
    }
}
