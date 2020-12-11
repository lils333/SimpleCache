//package com.lee.cache.serializer.protostuff;
//
//import com.lee.cache.exception.SerializerException;
//import com.lee.cache.serializer.BaseSerAndDeser;
//import com.lee.schema.Customer;
//import com.lee.schema.protostuff.CustomerSchema;
//import io.protostuff.LinkedBuffer;
//import io.protostuff.ProtostuffIOUtil;
//import lombok.extern.slf4j.Slf4j;
//
//@Slf4j
//public class CustomerSerializer extends BaseSerAndDeser<Customer> {
//
//    private final ThreadLocal<LinkedBuffer> BUFFER = ThreadLocal.withInitial(() -> LinkedBuffer.allocate(256));
//    private final CustomerSchema tokenSchema = new CustomerSchema();
//
//    @Override
//    public byte[] serialize(Customer token) {
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
//    public Customer deserialize(byte[] bytes) {
//        try {
//            Customer customer = tokenSchema.newMessage();
//            ProtostuffIOUtil.mergeFrom(bytes, customer, tokenSchema);
//            return customer;
//        } catch (Exception e) {
//            throw new DeserializerException(e);
//        }
//    }
//}
