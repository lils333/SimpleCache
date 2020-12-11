//package com.lee.cache.serializer.protostuff;
//
//import com.lee.cache.exception.DeserializerException;
//import com.lee.cache.exception.SerializerException;
//import com.lee.cache.serializer.BaseSerAndDeser;
//import com.lee.schema.Token;
//import com.lee.schema.protostuff.TokenSchema;
//import io.protostuff.LinkedBuffer;
//import io.protostuff.ProtostuffIOUtil;
//import lombok.extern.slf4j.Slf4j;
//
///**
// * 自定义Token的schema，来序列化
// *
// * @author l46li
// */
//@Slf4j
//public class TokenSerializer extends BaseSerAndDeser<Token> {
//
//    private final ThreadLocal<LinkedBuffer> BUFFER = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(256));
//    private final TokenSchema tokenSchema = new TokenSchema();
//
//    @Override
//    public byte[] serialize(Token token) {
//        LinkedBuffer buffer = BUFFER.get();
//        try {
//            return ProtostuffIOUtil.toByteArray(token, tokenSchema, buffer);
//        } catch (Exception e) {
//            throw new SerializerException(e);
//        } finally {
//            buffer.clear();
//        }
//    }
//
//    @Override
//    public Token deserialize(byte[] bytes) {
//        try {
//            Token token = tokenSchema.newMessage();
//            ProtostuffIOUtil.mergeFrom(bytes, token, tokenSchema);
//            return token;
//        } catch (Exception e) {
//            throw new DeserializerException(e);
//        }
//    }
//}
