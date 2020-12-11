package com.lee.cache.serializer;

public interface Serializer<S> {

    /**
     * 对于基本数据类型而言，为了减少byte[] 数组的创建，当前的byte[] 数组会复用，也就是序列化好的byte[] 数组需要先使用了，然后在
     * 继续调用
     *
     * @param key key to serizlize
     * @return byte[]
     */
    byte[] serialize(S key);

    S deserialize(byte[] bytes);

    Class<S> getType();
}
