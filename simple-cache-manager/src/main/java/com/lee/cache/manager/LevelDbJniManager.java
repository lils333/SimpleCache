package com.lee.cache.manager;

import com.lee.cache.Cache;
import com.lee.cache.LevelDbJniAnyCache;
import com.lee.cache.LevelDbJniCache;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.LevelDbJniConfiguration;
import com.lee.cache.exception.CacheException;
import com.lee.cache.serializer.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.fusesource.leveldbjni.JniDBFactory;
import org.iq80.leveldb.DB;
import org.iq80.leveldb.Options;

import java.io.File;
import java.io.IOException;

/**
 * @author l46li
 */
@Slf4j
public class LevelDbJniManager extends BaseCacheManager {

    @Override
    protected <K, V> Cache<K, V> createLevelDbJni(CacheConfiguration<K, V> configuration) {
        LevelDbJniConfiguration<K, V> config = (LevelDbJniConfiguration<K, V>) configuration;

        if (config.isAutoDetect()) {
            return new LevelDbJniAnyCache<>(createDb(config), config.getDefaultSerializer(),
                    config.isTruncate(),
                    config.path(),
                    configuration.name()
            );
        }

        Serializer<K> key = config.getSerializerKey();
        Serializer<V> value = config.getSerializerValue();

        LevelDbJniCache<K, V> levelDbCache;

        if (key != null && value != null) {
            levelDbCache = new LevelDbJniCache<>(createDb(config), key, value);
        } else {
            if (value != null) {
                levelDbCache = new LevelDbJniCache<>(createDb(config), config.key(), value);
            } else {
                levelDbCache = new LevelDbJniCache<>(createDb(config), config.key(), config.value());
            }
        }

        if (config.isTruncate()) {
            levelDbCache.truncate(config.isTruncate(), config.path(), configuration.name());
        }
        return levelDbCache;
    }

    private <K, V> DB createDb(LevelDbJniConfiguration<K, V> config) {
        DB db;
        try {
            db = JniDBFactory.factory.open(new File(config.path(), config.name()), createOptions(config));
        } catch (IOException e) {
            throw new CacheException("Can not create LevelDB for " + config.name(), e);
        }
        return db;
    }

    private <K, V> Options createOptions(LevelDbJniConfiguration<K, V> config) {
        return new Options()
                .createIfMissing(config.isCreatedIfMissing())
                .cacheSize(config.getCacheSize())
                .writeBufferSize(config.getWriteBufferSize())
                .verifyChecksums(config.isVerifyChecksums());
    }
}
