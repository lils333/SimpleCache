package com.lee.cache.serializer;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DefaultSerializer {

    public static final Serializer<Long> LONG_SERIALIZER = new LongSerializer();
    public static final Serializer<Double> DOUBLE_SERIALIZER = new DoubleSerializer();
    public static final Serializer<Float> FLOAT_SERIALIZER = new FloatSerializer();
    public static final Serializer<Character> CHARACTER_SERIALIZER = new CharacterSerializer();
    public static final Serializer<Short> SHORT_SERIALIZER = new ShortSerializer();
    public static final Serializer<Byte> BYTE_SERIALIZER = new ByteSerializer();
    public static final Serializer<Integer> INTEGER_SERIALIZER = new IntegerSerializer();
    public static final Serializer<Boolean> BOOLEAN_SERIALIZER = new BooleanSerializer();

//    public static void main(String[] args) {
//        IntegerSerializer2 integerSerializer2 = new IntegerSerializer2();
//
//        StopWatch stopWatch = new StopWatch();
//        for (int j = 0; j < 10; j++) {
//            stopWatch.reset();
//            stopWatch.start();
//            for (int i = 0; i < 100000000; i++) {
//                byte[] serialize = INTEGER_SERIALIZER.serialize(i);
//                INTEGER_SERIALIZER.deserialize(serialize);
//            }
//            for (int i = 0; i < 100000000; i++) {
//                byte[] serialize = integerSerializer2.serialize(i);
//                integerSerializer2.deserialize(serialize);
//            }
//            stopWatch.stop();
//            System.out.println("Cost : " + stopWatch.formatTime());
//        }
//    }

    private static class LongSerializer extends BaseSerAndDeser<Long> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Long value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(8);
            byteBuffer.clear();
            byteBuffer.putLong(value);
            return byteBuffer.array();
        }

        @Override
        public Long deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            return byteBuffer.getLong();
        }

        @Override
        public Class<Long> getType() {
            return Long.class;
        }
    }

    private static class DoubleSerializer extends BaseSerAndDeser<Double> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Double value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(8);
            byteBuffer.clear();
            byteBuffer.putDouble(value);
            return byteBuffer.array();
        }

        @Override
        public Double deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            return byteBuffer.getDouble();
        }

        @Override
        public Class<Double> getType() {
            return Double.class;
        }
    }

    private static class FloatSerializer extends BaseSerAndDeser<Float> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Float value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(4);
            byteBuffer.clear();
            byteBuffer.putFloat(value);
            return byteBuffer.array();
        }

        @Override
        public Float deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            return byteBuffer.getFloat();
        }

        @Override
        public Class<Float> getType() {
            return Float.class;
        }
    }

    private static class CharacterSerializer extends BaseSerAndDeser<Character> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Character value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(2);
            byteBuffer.clear();
            byteBuffer.putChar(value);
            return byteBuffer.array();
        }

        @Override
        public Character deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            return byteBuffer.getChar();
        }

        @Override
        public Class<Character> getType() {
            return Character.class;
        }
    }

    private static class ShortSerializer extends BaseSerAndDeser<Short> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Short value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(2);
            byteBuffer.clear();
            byteBuffer.putShort(value);
            return byteBuffer.array();
        }

        @Override
        public Short deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            return byteBuffer.getShort();
        }

        @Override
        public Class<Short> getType() {
            return Short.class;
        }
    }

    private static class ByteSerializer extends BaseSerAndDeser<Byte> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Byte value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(1);
            byteBuffer.clear();
            byteBuffer.put(value);
            return byteBuffer.array();
        }

        @Override
        public Byte deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            return byteBuffer.get();
        }

        @Override
        public Class<Byte> getType() {
            return Byte.class;
        }
    }

    private static class IntegerSerializer extends BaseSerAndDeser<Integer> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Integer value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(4);
            byteBuffer.clear();
            byteBuffer.putInt(value);
            return byteBuffer.array();
        }

        @Override
        public Integer deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            byteBuffer.flip();
            return byteBuffer.getInt();
        }

        @Override
        public Class<Integer> getType() {
            return Integer.class;
        }
    }

    private static class BooleanSerializer extends BaseSerAndDeser<Boolean> {

        private final ThreadLocal<Pool> pools = ThreadLocal.withInitial(RingBufferPool::new);

        @Override
        public byte[] serialize(Boolean value) {
            ByteBuffer byteBuffer = pools.get().getByteWithSize(1);
            byteBuffer.clear();
            byteBuffer.put(value ? (byte) 1 : 0);
            return byteBuffer.array();
        }

        @Override
        public Boolean deserialize(byte[] bytes) {
            ByteBuffer byteBuffer = ByteBuffer.wrap(bytes);
            byteBuffer.put(bytes);
            //向ByteBuffer里面写入了数据以后，那么必须要flip以后，才可以进行读取操作
            byteBuffer.flip();
            return byteBuffer.get() == 1 ? Boolean.TRUE : Boolean.FALSE;
        }

        @Override
        public Class<Boolean> getType() {
            return Boolean.class;
        }
    }

    interface Pool {
        ByteBuffer getByteWithSize(int byteSize);
    }

    /**
     * 为了不让每次都去创建byte[]数组，那么就把已经创建的byte[]先缓存上，然后采用RingBuffer的模式去重复使用
     * 一般的使用方式都是序列化以后，先处理，然后在继续序列化
     * <p>
     * 8 bytes  * 1024   ~= 8K
     * 4 bytes  * 1024   ~= 4K
     * <p>
     * 2bytes * 1024     ~= 2K
     * 1bytes * 1024    = 1K
     * <p>
     * 上面是单线程最多占用这么多的空间，如果是多个线程，那么在乘上线程数量
     * <p>
     * 该对象实际上主要是给ThreadLocal使用的，也就是为了不加锁，那么每个线程都有自己的缓存
     * <p>
     * 根据实际测试结果来看，该优化效率没有比直接基于现有的ByteBuffer包装一下，效率高多少，而且都是在可以接受的范围之内
     * 不外乎会频繁的创建了很多的ByteBuffer，一次mingc 就可以回收空间，所以这个地方为了简单也可以不使用该缓存
     */
    private static class RingBufferPool implements Pool {
        private static final int MAX_CACHE_SIZE = 1024;
        private int index = -1;
        private boolean isUsedCache = false;

        private final List<ByteBuffer> cached = new ArrayList<>(MAX_CACHE_SIZE);

        /**
         * 指定该基本数据类型需要占用的空间，比如
         * long ---8bytes  int ---4bytes char --2bytes short ---2bytes boolean ---1byte byte --1byte double --8bytes
         * float --4bytes
         *
         * @param byteSize 指定基本数据类型需要占用的bytes数
         * @return 返回缓存的bytes数组，如果不存在，就创建
         */
        @Override
        public ByteBuffer getByteWithSize(int byteSize) {
            index += 1;
            if (isUsedCache) {
                if (index >= MAX_CACHE_SIZE) {
                    index = 0;
                }
                return cached.get(index);
            } else {
                if (index >= MAX_CACHE_SIZE) {
                    index = 0;
                    isUsedCache = true;
                    return cached.get(index);
                } else {
                    ByteBuffer byteBuffer = ByteBuffer.wrap(new byte[byteSize]);
                    cached.add(byteBuffer);
                    return byteBuffer;
                }
            }
        }
    }
}
