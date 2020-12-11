package com.lee.cache.serializer.fst;

import com.lee.cache.exception.SerializerException;
import com.lee.cache.serializer.BaseSerAndDeser;
import lombok.extern.slf4j.Slf4j;
import org.nustaq.serialization.*;

/**
 * @author l46li
 */
@Slf4j
public class FstSerializer<S> extends BaseSerAndDeser<S> {

    /**
     * FSTConfiguration默认是线程安全的，所以全局使用了一个，唯一一个情况是频繁使用该类可能会由于锁导致性能下降，这个时候
     * 在用多个FSTConfiguration
     */
    private static final FSTConfiguration CONFUNSHAREDUNREGISTERED;

    static {
        CONFUNSHAREDUNREGISTERED = FSTConfiguration.createDefaultConfiguration();
        //如果想要和java兼用，那么就不能够设置成false
        CONFUNSHAREDUNREGISTERED.setShareReferences(false);
        //禁用Serializable或者Externalizable接口的检查
        CONFUNSHAREDUNREGISTERED.setStructMode(true);

        //如果提前知道需要序列化的类型，那么可以先把需要序列化的register上去，这样效率会更高，但是需要把所有的
        //需要序列化的对象都注册
        //注意：如果使用了显示注册类，如果在不同机器之间想要共享，就必须要保证这个注册的顺序是一致的，而且还要保证所有的FSTConfiguration
        //配置也是一致的，主要是在序列化和反序列化的时候注册了的类，不会把该类的信息写进去
    }

    private final ThreadLocal<FSTObjectInput> objectInput = new ThreadLocal<FSTObjectInput>() {
        @Override
        protected FSTObjectInput initialValue() {
            return new FSTObjectInputNoShared(CONFUNSHAREDUNREGISTERED);
        }
    };

    private final ThreadLocal<FSTObjectOutput> objectOutput = new ThreadLocal<FSTObjectOutput>() {
        @Override
        protected FSTObjectOutput initialValue() {
            return new FSTObjectOutputNoShared(CONFUNSHAREDUNREGISTERED);
        }
    };

    public FstSerializer(Class<S> type) {
        super(type);
    }

    @Override
    public byte[] serialize(S key) {
        FSTObjectOutput objectOutput = this.objectOutput.get();
        try {
            objectOutput.resetForReUse();
            objectOutput.writeObject(key);
            return objectOutput.getCopyOfWrittenBuffer();
        } catch (Exception e) {
            throw new SerializerException("Can not serialize object " + key, e);
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public S deserialize(byte[] bytes) {
        FSTObjectInput objectInput = this.objectInput.get();
        try {
            objectInput.resetForReuseUseArray(bytes);
            return (S) objectInput.readObject();
        } catch (Exception e) {
            throw new SerializerException("Can not deserialize from bytes with " + CONFUNSHAREDUNREGISTERED, e);
        }
    }
}
