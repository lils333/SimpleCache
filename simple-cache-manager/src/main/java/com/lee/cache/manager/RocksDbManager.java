package com.lee.cache.manager;

import com.lee.cache.Cache;
import com.lee.cache.RocksDbAnyCache;
import com.lee.cache.RocksDbCache;
import com.lee.cache.config.CacheConfiguration;
import com.lee.cache.config.RocksDbConfiguration;
import com.lee.cache.exception.CacheException;
import com.lee.cache.serializer.Serializer;
import lombok.extern.slf4j.Slf4j;
import org.rocksdb.*;
import org.rocksdb.util.SizeUnit;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

/**
 * @author l46li
 */
@Slf4j
public class RocksDbManager extends BaseCacheManager {

    private List<Options> options = new ArrayList<>(10);

    @Override
    protected <K, V> Cache<K, V> createRocksDb(CacheConfiguration<K, V> configuration) {
        RocksDbConfiguration<K, V> config = (RocksDbConfiguration<K, V>) configuration;

        RocksDB db = createInternalDb(config);
        if (config.isAutoDetect()) {
            return new RocksDbAnyCache<>(db, config.getDefaultSerializer(),
                    config.isTruncate(),
                    config.path(),
                    configuration.name()
            );
        }

        Serializer<K> key = config.getSerializerKey();
        Serializer<V> value = config.getSerializerValue();

        RocksDbCache<K, V> rocksDb;
        if (key != null && value != null) {
            rocksDb = new RocksDbCache<>(db, key, value);
        } else {
            if (value != null) {
                rocksDb = new RocksDbCache<>(db, config.key(), value);
            } else {
                rocksDb = new RocksDbCache<>(db, config.key(), config.value());
            }
        }

        if (config.isTruncate()) {
            rocksDb.truncate(config.isTruncate(), config.path(), config.name());
        }

        return rocksDb;
    }

    @Override
    public void close() {
        super.close();
        for (Options option : options) {
            try {
                option.close();
            } catch (Exception e) {
                log.error("Can not close Options " + option);
            }
        }
        options = null;
    }

    private <K, V> RocksDB createInternalDb(RocksDbConfiguration<K, V> config) {
        if (config.getTimeToLiveSeconds() != 0) {
            return createTtlDb(config);
        } else {
            return createDb(config);
        }
    }

    private <K, V> RocksDB createDb(RocksDbConfiguration<K, V> config) {
        try {
            Options option = buildBasicOptions(config);
            if (option != null) {
                options.add(option);
                return RocksDB.open(option, config.path() + File.separator + config.name());
            }
            throw new RocksDBException("Can not crate Options for RocksDB");
        } catch (RocksDBException e) {
            throw new CacheException("Can not create RocksDB for " + config.path(), e);
        }
    }

    private <K, V> TtlDB createTtlDb(RocksDbConfiguration<K, V> config) {
        try {
            Options option = buildBasicOptions(config);
            if (option != null) {
                options.add(option);
                return TtlDB.open(
                        option,
                        config.path() + File.separator + config.name()
                );
            }
            throw new RocksDBException("Can not crate Options for RocksDB");
        } catch (RocksDBException e) {
            throw new CacheException("Can not create RocksDB for " + config.path(), e);
        }
    }

    private <K, V> Options buildBasicOptions(RocksDbConfiguration<K, V> config) {
        /*
         * 在 level 0 和 level 1 之间的 compaction 比较 tricky，
         * level 0 会覆盖所有的 key range，所以当 level 0 和 level 1 之间开始进行 compaction 的时候，
         * 所有的 level 1 的文件都会参与合并。这时候就不能处理 level 1 到 level 2 的 compaction，
         * 必须等到 level 0 到 level 1 的 compaction 完成，才能继续。
         * 如果 level 0 到 level 1 的速度比较慢，那么就可能导致整个系统大多数时候只有一个 compaction 在进行。
         *
         * Level 0 到 level 1 的 compaction 是一个单线程的，
         * 也就意味着这个操作其实并不快，RocksDB 后续引入了一个 max_subcompactions，
         * 解决了 level 0 到 level 1 的 compaction 多线程问题。
         * 通常，为了加速 level0 到 level1 的 compaction，我们会尽量保证level 0 和 level 1 有相同的size
         */
        return new Options()
                .setTtl(config.getTimeToLiveSeconds())
                .setCreateIfMissing(config.isCreatedIfMissing())
                //指定memtable的最大大小，我们知道memtable实际上是保存在内存里面的一个skiplist，当一个memtable
                //满了以后，会把该memtable设置成immutable ，然后等待flush到level0里面去
                //这个地方就是指定一个memtable的大小，就是WriteBufferSize
                .setWriteBufferSize(config.getWriteBufferSize())

                //这个地方指定的就是个数，也就是一共可以有多少个memtable，注意这个是memtable 和 immutable的总和
                //如果他们总和到达了这个值了以后，那么就会阻塞写入，通常阻塞都是由于flush不及时导致的，如果写入过快
                //那么就可能导致阻塞，设置该值过大，会导致暂用内存过大
                .setMaxWriteBufferNumber(config.getMaxWriteBufferNumber())

                //在 flush 到 level 0 之前，最少需要被 merge 的 memtable 个数。
                //如果这个值是 2，那么当至少有两个 immutable 的 memtable 的时候，
                //RocksDB 会将这两个 immutable memtable 先 merge，
                //在 flush 到 level 0。预先 merge 能减小需要写入的 key 的数据，
                //譬如一个 key 在不同的 memtable 里面都有修改，那么我们可以 merge 成一次修改。
                //但这个值太大了会影响读取性能，因为 Get 会遍历所有的 memtable 来看这个 key 是否存在
                .setMinWriteBufferNumberToMerge(1)

                //当 level0 的文件数据达到这个值的时候，就开始进行 level0 到 level1 的 compaction。
                //所以通常 level 0 的大小就是
                //level0总大小 = write_buffer_size * min_write_buffer_number_to_merge * level0_file_num_compaction_trigger
                .setLevel0FileNumCompactionTrigger(4)
                .setLevel0SlowdownWritesTrigger(16)
                .setLevel0StopWritesTrigger(24)

                // 设置level1 的总大小，在上面提到，我们通常建议 level1 跟 level0 的size 相当
                .setMaxBytesForLevelBase(160 * SizeUnit.MB)

                //上层的 level 的 size 每层都会比当前层大 max_bytes_for_level_multiplier 倍，
                //这个值默认是 10，通常也不建议修改,也就是leveln 是leveln-1的10倍
                .setMaxBytesForLevelMultiplier(10)

                //target_file_size_base 则是 level1 单个SST文件的size。
                //上面层的文件size 都会比当前层大target_file_size_multiplier 倍
                //设置了它，那么就默认指定了sst文件的大小了
                //增加 target_file_size_base 会减少整个 DB 的 size，这通常是一件好事情，
                //也通常建议 target_file_size_base 等于 max_bytes_for_level_base / 10，也就是 level 1 会有 10 个 SST 文件
                .setTargetFileSizeBase(16 * SizeUnit.MB)

                //所有层的sst文件是一样大的,默认值，通常也不建议修改
                .setTargetFileSizeMultiplier(1)

                //设置后台的flush和compact线程的数量
                .setEnv(Env.getDefault().setBackgroundThreads(4, Priority.LOW))

                //设置压缩规制
                .setCompactionStyle(CompactionStyle.LEVEL)
                .setCompressionType(CompressionType.SNAPPY_COMPRESSION)

                //设置level的最大层级 默认就是7层
                .setNumLevels(7)
                .setUseDirectReads(false)
                .setAllowMmapReads(false)
                .setAllowMmapWrites(false)
                .setLevelCompactionDynamicLevelBytes(true)

                //设置blockcache
                .setTableFormatConfig(buildTableConfig(config))
                .setMaxOpenFiles(config.getMaxOpenFiles());
    }

    private <K, V> TableFormatConfig buildTableConfig(RocksDbConfiguration<K, V> config) {
        return new BlockBasedTableConfig()
                //获取每一个key的前几个字节来判断当前key是否存储哎某个sst文件中，10表示允许1%的误判，设置值越大
                //误判率越高，但是暂用内存也会越大，注意他主要是为随机查询提供少访问的可能
                //一旦BloomFilter判断该key不在某个sst文件里面，那么它肯定就不在
                //如果它判断key可能在某个文件里面，那么它可能不存在，这个就是误判率
                .setFilterPolicy(new BloomFilter(10))

                //一个block的大小，sst文件里面是按照block一个快一个快来组织数据的，也就是一个sst文件里面包含了很多个
                //block快，当然在访问的时候也就会加载该block的内存到内存里面去，其实不用太大，因为太大的话随机访问的时候
                //加载一个快到内存里面，然后在二分查找，其实没有必要，加载blocksize大小到内存里面去，但是只访问一个,有点儿浪费
                //改成默认4K就可以了，如果我们主要使用iter来访问数据，还可以设置小一些，但是不能够小于1K
                .setBlockSize(4 * SizeUnit.KB)
                //如果当前blocksize只还有少于或者等于5%的空间可用，但是添加一个新的记录进去会超过当前blocksize的大小，
                //那么创建一个新的blocksize，之前的就关闭掉
                .setBlockSizeDeviation(10)

                //设置读取数据的时候的缓存空间总大小，缓存的都是未压缩的数据 建议值是： 1/3指定的JVM内存
                //block 其实也是加快随机查询速度的，每一个随机查询的时候需要把该cache的值存放到该blockcache里面去
                //当指定的内存大小满了以后采用LRU算法来驱逐数据
                .setBlockCache(new LRUCache(config.getCacheSize()))

                //是否把index(key + offset + value size) 和 filter(bloom filter 10 bits per key)放到block cache里面去
                //默认是不会把index 和 filter放到block cache里面去的，如果放到blockcache里面去，那么用于给cache数据的空间就少了
                //虽然大了一点儿，但是查询的速度就要比之前快很多,看情况来决定是否开启
                .setCacheIndexAndFilterBlocks(true)
                //配置了上面的CacheIndexAndFilterBlocks必须要也要把这个参数开启才会生效
                .setPinL0FilterAndIndexBlocksInCache(true)

                //关闭checksum的校验，也就是不对文件进行校验
                .setChecksumType(config.isVerifyChecksums() ? ChecksumType.kxxHash : ChecksumType.kNoChecksum)

                //这个就是查询的时候使用的方式了，二分查找
                .setIndexType(IndexType.kBinarySearch);
    }
}
