package com.lee.cache;

import com.lee.cache.exception.CacheException;
import com.lee.cache.serializer.Serializer;
import com.protonail.leveldb.jna.*;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 实际上leveldb是没有update和delete的操作的，他们都是一条记录而已，删除只是添加一条记录，该记录有一个标志表示删除
 * 更新也只是添加一条新的记录，由于leveledb是基于LSM算法的，也就是说在获取的时候它会先获取最新鲜的数据
 * <p>
 * 注意LSM算法的写放大问题
 *
 * @param <K>
 * @param <V>
 * @author l46li
 */
@Slf4j
public class LevelDbJnaAnyCache<K, V> extends BaseCache<K, V> {

    private LevelDB levelDb;
    private boolean isTruncate;
    private String cachePath;
    private String name;

    private LevelDBReadOptions readOption = new LevelDBReadOptions();
    private LevelDBWriteOptions writeOption = new LevelDBWriteOptions();

    public LevelDbJnaAnyCache(LevelDB db, Serializer<Object> serializer, boolean truncate, String path, String name) {
        super(serializer);
        this.levelDb = db;
        this.isTruncate = truncate;
        this.cachePath = path;
        this.name = name;
    }

    /**
     * @param key key to get value
     * @return value
     */
    @Override
    @SuppressWarnings("unchecked")
    public V get(K key) {
        try {
            byte[] bytes = levelDb.get(serializeAny(key), readOption);
            if (bytes != null) {
                return (V) deserializeAny(bytes);
            }
        } catch (LevelDBException e) {
            throw new CacheException("Can not get data from LevelDB : " + levelDb, e);
        }
        return empty;
    }

    /**
     * LevelDBWriteOptions是不能够有snapshot，因为写都应该在主版本里面才可以,
     * 同理LevelDBWriteOptions应该也是指定写入的一个行为，应该可以全局使用一个LevelDBWriteOptions
     * 并且leveldb已经事先多并发写入的问题，所以可以不用加锁，不过iterator或者batch的时候不能够多线程使用
     *
     * @param key   key
     * @param value value
     * @return empty
     */
    @Override
    public V put(K key, V value) {
        try {
            levelDb.put(serializeAny(key), serializeAny(value), writeOption);
        } catch (LevelDBException e) {
            throw new CacheException("Can not put data to LevelDB : " + levelDb, e);
        }
        return empty;
    }

    @Override
    public V delete(K key) {
        try {
            levelDb.delete(serializeAny(key), writeOption);
        } catch (LevelDBException e) {
            throw new CacheException("Can not delete data from LevelDB : " + levelDb, e);
        }
        return empty;
    }

    @Override
    public void put(Map<K, V> keyValues) {
        try (LevelDBWriteOptions writeOptions = new LevelDBWriteOptions()) {
            try (LevelDBWriteBatch batch = new LevelDBWriteBatch()) {
                for (Map.Entry<K, V> entries : keyValues.entrySet()) {
                    batch.put(
                            serializeAny(entries.getKey()),
                            serializeAny(entries.getValue())
                    );
                }
                levelDb.write(batch, writeOptions);
            } catch (LevelDBException e) {
                throw new CacheException("Can not execute batch put data to LevelDB : " + levelDb, e);
            }
        }
    }

    @Override
    public void delete(Collection<K> keys) {
        try (LevelDBWriteOptions writeOptions = new LevelDBWriteOptions()) {
            try (LevelDBWriteBatch batch = new LevelDBWriteBatch()) {
                for (K key : keys) {
                    batch.delete(serializeAny(key));
                }
                levelDb.write(batch, writeOptions);
            } catch (LevelDBException e) {
                throw new CacheException("Can not execute batch delete data from LevelDB : " + levelDb, e);
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void consumeKey(Consumer<K> consumer) {
        try (LevelDBReadOptions levelDbReadOption = new LevelDBReadOptions()) {
            levelDbReadOption.setFillCache(false);
            levelDbReadOption.setSnapshot(levelDb.createSnapshot());
            try (LevelDBKeyIterator iterator = new LevelDBKeyIterator(levelDb, levelDbReadOption)) {
                while (iterator.hasNext()) {
                    consumer.accept((K) deserializeAny(iterator.next()));
                }
            } finally {
                closeQuit(levelDbReadOption.getSnapshot());
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void consumeValue(Consumer<V> consumer) {
        try (LevelDBReadOptions levelDbReadOption = new LevelDBReadOptions()) {
            levelDbReadOption.setFillCache(false);
            levelDbReadOption.setSnapshot(levelDb.createSnapshot());
            try (LevelDBKeyValueIterator iterator = new LevelDBKeyValueIterator(levelDb, levelDbReadOption)) {
                while (iterator.hasNext()) {
                    KeyValuePair pair = iterator.next();
                    consumer.accept((V) deserializeAny(pair.getValue()));
                }
            } finally {
                closeQuit(levelDbReadOption.getSnapshot());
            }
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void consume(BiConsumer<K, V> consumer) {
        try (LevelDBReadOptions levelDbReadOption = new LevelDBReadOptions()) {
            levelDbReadOption.setFillCache(false);
            levelDbReadOption.setSnapshot(levelDb.createSnapshot());
            try (LevelDBKeyValueIterator iterator = new LevelDBKeyValueIterator(levelDb, levelDbReadOption)) {
                while (iterator.hasNext()) {
                    KeyValuePair pair = iterator.next();
                    consumer.accept(
                            (K) deserializeAny(pair.getKey()),
                            (V) deserializeAny(pair.getValue())
                    );
                }
            } finally {
                closeQuit(levelDbReadOption.getSnapshot());
            }
        }
    }

    /**
     * 如果需要的话，那么继承close，显示的关闭掉
     *
     * @throws IOException 关闭不了抛出异常
     */
    @Override
    public void close() throws IOException {
        try {
            closeQuit(readOption, writeOption, levelDb);
            levelDb = null;
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
