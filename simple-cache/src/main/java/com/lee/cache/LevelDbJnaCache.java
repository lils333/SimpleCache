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

@Slf4j
public class LevelDbJnaCache<K, V> extends BaseCache<K, V> {

    private LevelDB levelDb;
    private boolean isTruncate;
    private String cachePath;
    private String name;

    private LevelDBReadOptions readOption = new LevelDBReadOptions();
    private LevelDBWriteOptions writeOption = new LevelDBWriteOptions();

    public LevelDbJnaCache(LevelDB db, Class<K> keyType, Serializer<V> serializerValue) {
        super(keyType, serializerValue);
        this.levelDb = db;
    }

    public LevelDbJnaCache(LevelDB db, Class<K> keyType, Class<V> valueType) {
        super(keyType, valueType);
        this.levelDb = db;
    }

    public LevelDbJnaCache(LevelDB db, Serializer<K> serializerKey, Serializer<V> serializerValue) {
        super(serializerKey, serializerValue);
        this.levelDb = db;
    }

    /**
     * 不需要在LevelDBReadOptions上面添加一个snapshot，也就是从主版本里面获取信息,
     * 因为每次LevelDBReadOptions都需要关闭和重新创建，频繁创建不好
     * TODO：可不可以使用全局的单例LevelDBReadOptions来进行读取操作，因为没有snapshot，所以感觉应该是可以的，待验证
     * 我的理解是创建一个LevelDBReadOptions，然后所有的读取操作都会按照LevelDBReadOptions指定的行为来获取数据
     * 可以在最后不在使用该行为的时候把该LevelDBReadOptions删除掉
     *
     * @param key key to get value
     * @return value
     */
    @Override
    public V get(K key) {
        try {
            byte[] bytes = levelDb.get(serializeKey(key), readOption);
            if (bytes != null) {
                return deserializeValue(bytes);
            }
        } catch (LevelDBException e) {
            throw new CacheException("Can not get data from LevelDB : " + levelDb, e);
        }
        return empty;
    }

    /**
     * LevelDBWriteOptions是不能够有snapshot，因为写都应该在主版本里面才可以,
     * 同理LevelDBWriteOptions应该也是指定写入的一个行为，应该可以全局使用一个LevelDBWriteOptions
     *
     * @param key   key
     * @param value value
     * @return empty
     */
    @Override
    public V put(K key, V value) {
        try {
            levelDb.put(serializeKey(key), serializeValue(value), writeOption);
        } catch (LevelDBException e) {
            throw new CacheException("Can not put data to LevelDB : " + levelDb, e);
        }
        return empty;
    }

    @Override
    public V delete(K key) {
        //实际上leveldb是没有update和delete的操作的，他们都是一条记录而已，删除只是添加一条记录，该记录有一个标志表示删除
        //g更新也只是添加一条新的记录，由于leveledb是基于LSM算法的，也就是说在获取的时候它会先获取最新鲜的数据
        try {
            levelDb.delete(serializeKey(key), writeOption);
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
                            serializeKey(entries.getKey()),
                            serializeValue(entries.getValue())
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
                    batch.delete(serializeKey(key));
                }
                levelDb.write(batch, writeOptions);
            } catch (LevelDBException e) {
                throw new CacheException("Can not execute batch delete data from LevelDB : " + levelDb, e);
            }
        }
    }

    @Override
    public void consumeKey(Consumer<K> consumer) {
        try (LevelDBReadOptions levelDbReadOption = new LevelDBReadOptions()) {
            levelDbReadOption.setFillCache(false);
            levelDbReadOption.setSnapshot(levelDb.createSnapshot());
            try (LevelDBKeyIterator iterator = new LevelDBKeyIterator(levelDb, levelDbReadOption)) {
                while (iterator.hasNext()) {
                    consumer.accept(deserializeKey(iterator.next()));
                }
            } finally {
                closeQuit(levelDbReadOption.getSnapshot());
            }
        }
    }

    @Override
    public void consumeValue(Consumer<V> consumer) {
        try (LevelDBReadOptions levelDbReadOption = new LevelDBReadOptions()) {
            levelDbReadOption.setFillCache(false);
            levelDbReadOption.setSnapshot(levelDb.createSnapshot());
            //leveldbjna has been seek_to_first, so current no need to seek
            try (LevelDBKeyValueIterator iterator = new LevelDBKeyValueIterator(levelDb, levelDbReadOption)) {
                while (iterator.hasNext()) {
                    KeyValuePair pair = iterator.next();
                    consumer.accept(deserializeValue(pair.getValue()));
                }
            } finally {
                closeQuit(levelDbReadOption.getSnapshot());
            }
        }
    }

    @Override
    public void consume(BiConsumer<K, V> consumer) {
        try (LevelDBReadOptions levelDbReadOption = new LevelDBReadOptions()) {
            levelDbReadOption.setFillCache(false);
            levelDbReadOption.setSnapshot(levelDb.createSnapshot());
            try (LevelDBKeyValueIterator iterator = new LevelDBKeyValueIterator(levelDb, levelDbReadOption)) {
                while (iterator.hasNext()) {
                    KeyValuePair pair = iterator.next();
                    consumer.accept(
                            deserializeKey(pair.getKey()),
                            deserializeValue(pair.getValue())
                    );
                }
            } finally {
                closeQuit(levelDbReadOption.getSnapshot());
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
