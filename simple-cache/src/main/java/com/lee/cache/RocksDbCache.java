package com.lee.cache;

import com.lee.cache.exception.CacheException;
import com.lee.cache.serializer.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.rocksdb.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * @author l46li
 */
@Slf4j
public class RocksDbCache<K, V> extends BaseCache<K, V> {

    static {
        RocksDB.loadLibrary();
    }

    private RocksDB db;
    private boolean isTruncate;
    private String cachePath;
    private String name;

    private ReadOptions readOption = new ReadOptions();
    private WriteOptions writeOption = new WriteOptions();

    public RocksDbCache(RocksDB db, Class<K> keyType, Serializer<V> serializerValue) {
        super(keyType, serializerValue);
        this.db = db;
    }

    public RocksDbCache(RocksDB db, Serializer<K> serializerKey, Serializer<V> serializerValue) {
        super(serializerKey, serializerValue);
        this.db = db;
    }

    public RocksDbCache(RocksDB db, Class<K> keyType, Class<V> valueType) {
        super(keyType, valueType);
        this.db = db;
    }

    @Override
    public V get(K key) {
        try {
            byte[] bytes = db.get(readOption, serializeKey(key));
            if (bytes != null) {
                return deserializeValue(bytes);
            }
        } catch (RocksDBException e) {
            throw new CacheException("Can not read data from RocksDB : " + db, e);
        }
        return empty;
    }

    @Override
    public V put(K key, V value) {
        try {
            db.put(writeOption, serializeKey(key), serializeValue(value));
        } catch (RocksDBException e) {
            throw new CacheException("Can not write data to RocksDB : " + db, e);
        }
        return empty;
    }

    @Override
    public V delete(K key) {
        try {
            db.delete(writeOption, serializeKey(key));
        } catch (RocksDBException e) {
            throw new CacheException("Can not delete data from RocksDB : " + db, e);
        }
        return empty;
    }

    @Override
    public void put(Map<K, V> keyValues) {
        try (WriteOptions writeOptions = new WriteOptions()) {
            try (WriteBatch batch = new WriteBatch()) {
                for (Map.Entry<K, V> entry : keyValues.entrySet()) {
                    batch.put(
                            serializeKey(entry.getKey()),
                            serializeValue(entry.getValue())
                    );
                }
                db.write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new CacheException("Can not execute put batch data to RocksDB : " + db, e);
            }
        }
    }

    @Override
    public void delete(Collection<K> keys) {
        try (WriteOptions writeOptions = new WriteOptions()) {
            try (WriteBatch batch = new WriteBatch()) {
                for (K key : keys) {
                    batch.delete(serializeKey(key));
                }
                db.write(writeOptions, batch);
            } catch (RocksDBException e) {
                throw new CacheException("Can not execute delete batch data from RocksDB : " + db, e);
            }
        }
    }

    @Override
    public void consumeKey(Consumer<K> consumer) {
        try (ReadOptions rocksDbReadOption = new ReadOptions()) {
            rocksDbReadOption.setFillCache(false).setVerifyChecksums(false).setSnapshot(db.getSnapshot());
            RocksIterator iterator = db.newIterator(rocksDbReadOption);
            try {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    consumer.accept(deserializeKey(iterator.key()));
                }
            } finally {
                closeQuit(iterator, rocksDbReadOption.snapshot());
            }
        }
    }

    @Override
    public void consumeValue(Consumer<V> consumer) {
        try (ReadOptions rocksDbReadOption = new ReadOptions()) {
            rocksDbReadOption.setFillCache(false).setVerifyChecksums(false).setSnapshot(db.getSnapshot());
            RocksIterator iterator = db.newIterator(rocksDbReadOption);
            try {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    consumer.accept(deserializeValue(iterator.value()));
                }
            } finally {
                closeQuit(iterator, rocksDbReadOption.snapshot());
            }
        }
    }

    @Override
    public void consume(BiConsumer<K, V> consumer) {
        try (ReadOptions rocksDbReadOption = new ReadOptions()) {
            rocksDbReadOption.setFillCache(false).setVerifyChecksums(false).setSnapshot(db.getSnapshot());
            RocksIterator iterator = db.newIterator(rocksDbReadOption);
            try {
                for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
                    consumer.accept(deserializeKey(iterator.key()), deserializeValue(iterator.value()));
                }
            } finally {
                closeQuit(iterator, rocksDbReadOption.snapshot());
            }
        }
    }

    public void truncate(boolean truncate, String cachePath, String name) {
        this.isTruncate = truncate;
        this.cachePath = cachePath;
        this.name = name;
    }

    /**
     * 如果需要的话，那么继承close，显示的关闭掉
     *
     * @throws IOException 关闭不了抛出异常
     */
    @Override
    public void close() throws IOException {
        try {
            closeQuit(readOption, writeOption, db);
            db = null;
            readOption = null;
            writeOption = null;
        } finally {
            if (isTruncate) {
                FileUtils.forceDelete(new File(cachePath, name));
            }
            cachePath = null;
            name = null;
        }
    }
}
