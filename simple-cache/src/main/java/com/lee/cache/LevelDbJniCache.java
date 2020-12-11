package com.lee.cache;

import com.lee.cache.exception.CacheException;
import com.lee.cache.serializer.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.iq80.leveldb.*;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.Consumer;

/**
 * 针对leveldb而言，它的所有操作都是向db里面插入一下record而已，删除操作也是向里面插入一条record，只是这条记录里面
 * 的标志位位0，表示删除，修改和新增是同一个逻辑
 * <p>
 * 获取操作首先从memtable --> imemtable --> level0 -->level1 ........--->levelN
 * 按照这顺序获取，那么获取的就是最新的数据，也就是之前的数据就认为已经老了，再compact的时候会回收掉
 *
 * @param <K> 需要序列化的key，主要是leveldb里面 存储的都是字节数组
 * @param <V> 需要序列化的value
 */
@Slf4j
public class LevelDbJniCache<K, V> extends BaseCache<K, V> {

    private DB db;
    private boolean isTruncate;
    private String cachePath;
    private String name;

    private ReadOptions readOption = new ReadOptions();
    private WriteOptions writeOption = new WriteOptions();

    public LevelDbJniCache(DB db, Class<K> keyType, Serializer<V> serializerValue) {
        super(keyType, serializerValue);
        this.db = db;
    }

    public LevelDbJniCache(DB db, Class<K> keyType, Class<V> valueType) {
        super(keyType, valueType);
        this.db = db;
    }

    public LevelDbJniCache(DB db, Serializer<K> serializerKey, Serializer<V> serializerValue) {
        super(serializerKey, serializerValue);
        this.db = db;
    }

    /**
     * 随机读取需要开启fillCache，这样可以把随机获取的值缓存到内存里面去
     *
     * @param key 需要查询的key
     * @return 返回value
     */
    @Override
    public V get(K key) {
        try {
            byte[] bytes = db.get(serializeKey(key), readOption);
            if (bytes != null) {
                return deserializeValue(bytes);
            }
        } catch (DBException e) {
            throw new CacheException("Can not get data from LevelDB : " + db, e);
        }
        return empty;
    }

    /**
     * 注意: 不能够在写上面添加snapshot，因为写必须写在主版本上面去
     *
     * @param key   need update key
     * @param value need update value
     * @return empty
     */
    @Override
    public V put(K key, V value) {
        try {
            db.put(serializeKey(key), serializeValue(value), writeOption);
        } catch (DBException e) {
            throw new CacheException("Can not put data to LevelDB : " + db, e);
        }
        return empty;
    }

    @Override
    public V delete(K key) {
        try {
            db.delete(serializeKey(key), writeOption);
        } catch (DBException e) {
            throw new CacheException("Can not delete data from LevelDB : " + db, e);
        }
        return empty;
    }

    @Override
    public void put(Map<K, V> keyValues) {
        WriteBatch batch = db.createWriteBatch();
        try {
            for (Map.Entry<K, V> entry : keyValues.entrySet()) {
                batch.put(
                        serializeKey(entry.getKey()),
                        serializeValue(entry.getValue())
                );
            }
            db.write(batch, writeOption);
        } catch (DBException e) {
            throw new CacheException("Can not execute put batch data to LevelDB : " + db, e);
        } finally {
            closeQuit(batch);
        }
    }

    @Override
    public void delete(Collection<K> keys) {
        WriteBatch batch = db.createWriteBatch();
        try {
            for (K key : keys) {
                batch.delete(serializeKey(key));
            }
            db.write(batch, writeOption);
        } catch (DBException e) {
            throw new CacheException("Can not execute delete batch data from LevelDB : " + db, e);
        } finally {
            closeQuit(batch);
        }
    }

    @Override
    public void consumeKey(Consumer<K> consumer) {
        ReadOptions levelDbReadOption = new ReadOptions().fillCache(false).snapshot(db.getSnapshot());
        DBIterator iterator = db.iterator(levelDbReadOption);
        try {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                consumer.accept(deserializeKey(iterator.peekNext().getKey()));
            }
        } finally {
            closeQuit(iterator, levelDbReadOption.snapshot());
        }
    }

    /**
     * 默认iter会在底层创建一个snapshot，所以这个地方显示的指定一个snapshot
     *
     * @param consumer 需要消费的consumer
     */
    @Override
    public void consumeValue(Consumer<V> consumer) {
        ReadOptions levelDbReadOption = new ReadOptions().fillCache(false).snapshot(db.getSnapshot());
        DBIterator iterator = db.iterator(levelDbReadOption);
        try {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                consumer.accept(deserializeValue(iterator.peekNext().getValue()));
            }
        } finally {
            closeQuit(iterator, levelDbReadOption.snapshot());
        }
    }

    @Override
    public void consume(BiConsumer<K, V> consumer) {
        ReadOptions levelDbReadOption = new ReadOptions().fillCache(false).snapshot(db.getSnapshot());
        DBIterator iterator = db.iterator(levelDbReadOption);
        try {
            for (iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
                consumer.accept(
                        deserializeKey(iterator.peekNext().getKey()),
                        deserializeValue(iterator.peekNext().getValue())
                );
            }
        } finally {
            closeQuit(iterator, levelDbReadOption.snapshot());
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
            if (db != null) {
                closeQuit(db);
                db = null;
                readOption = null;
                writeOption = null;
            }
        } finally {
            if (isTruncate) {
                FileUtils.deleteDirectory(new File(cachePath, name));
            }
            cachePath = null;
            name = null;
        }
    }

    public void truncate(boolean truncate, String cachePath, String name) {
        this.isTruncate = truncate;
        this.cachePath = cachePath;
        this.name = name;
    }
}
