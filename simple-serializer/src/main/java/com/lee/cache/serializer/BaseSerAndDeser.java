package com.lee.cache.serializer;

public abstract class BaseSerAndDeser<S> implements Serializer<S> {

    private Class<S> clazz;

    public BaseSerAndDeser() {
    }

    public BaseSerAndDeser(Class<S> clazz) {
        this.clazz = clazz;
    }

    @Override
    public Class<S> getType() {
        return this.clazz;
    }

    @Override
    public byte[] serialize(S key) {
        return new byte[0];
    }

    @Override
    public S deserialize(byte[] bytes) {
        return null;
    }
}
