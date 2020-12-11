package com.lee.cache.serializer.protostuff;

import com.lee.cache.exception.SerializerException;
import com.lee.cache.serializer.BaseSerAndDeser;
import io.protostuff.LinkedBuffer;
import io.protostuff.ProtostuffIOUtil;
import io.protostuff.Schema;
import lombok.extern.slf4j.Slf4j;

/**
 * 由于protostuff序列化的时候不需要类型信息，它使用的Schema，也就是在运行的时候动态生成Schema，并缓存
 * 所以它不方便来序列化任何对象类型，因为反序列化的时候需要指定对象的类型，除非反序列化提供一个类型
 *
 * @param <S>
 * @author l46li
 */
@Slf4j
public class ProtoStuffSerializer<S> extends BaseSerAndDeser<S> {

    private final ThreadLocal<LinkedBuffer> bufferCache = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(1024));
    private final Schema<S> schema;

    public ProtoStuffSerializer(Class<S> type) {
        super(type);
        schema = SchemaFactory.createFrom(type);
    }

    @Override
    public byte[] serialize(S key) {
        LinkedBuffer buffer = bufferCache.get();
        try {
            //可能序列化的大小超过了指定大小，那么会抛出异常
            return ProtostuffIOUtil.toByteArray(key, schema, buffer);
        } catch (Exception e) {
            throw new SerializerException("Can not serialize object : " + key, e);
        } finally {
            buffer.clear();
        }
    }

    @Override
    public S deserialize(byte[] bytes) {
        try {
            S value = schema.newMessage();
            ProtostuffIOUtil.mergeFrom(bytes, value, schema);
            return value;
        } catch (Exception e) {
            throw new SerializerException("Can not deserialize from bytes with schema " + schema, e);
        }
    }
}
