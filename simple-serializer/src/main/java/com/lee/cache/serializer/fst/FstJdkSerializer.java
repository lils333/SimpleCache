package com.lee.cache.serializer.fst;

import com.lee.cache.exception.SerializerException;
import com.lee.cache.serializer.BaseSerAndDeser;
import lombok.extern.slf4j.Slf4j;
import org.nustaq.serialization.FSTConfiguration;
import org.nustaq.serialization.FSTObjectInput;
import org.nustaq.serialization.FSTObjectOutput;

/**
 * 它应该算是最好的序列化工具了，兼用JDK的时候可以序列化任何信息对象，如果我们在序列化的时候有一定的要求的
 * 比如不用事先知道需要序列化的类的信息，而且只要是集成了Serializable或者Externalizable就可以序列化
 * 而且只要对象里面从写了readObject和writeObject方法的话，那么就算是瞬态对象也可以序列化
 *
 * @author l46li
 */
@Slf4j
public class FstJdkSerializer extends BaseSerAndDeser<Object> {

    private static final FSTConfiguration CONFIGURATION = FSTConfiguration.createDefaultConfiguration();
    private static final ThreadLocal<FSTObjectInput> OBJECTINPUT = ThreadLocal.withInitial(() -> new FSTObjectInput(CONFIGURATION));
    private static final ThreadLocal<FSTObjectOutput> OBJECTOUTPUT = ThreadLocal.withInitial(() -> new FSTObjectOutput(CONFIGURATION));

    @Override
    public byte[] serialize(Object object) {
        FSTObjectOutput objectOutput = OBJECTOUTPUT.get();
        try {
            objectOutput.resetForReUse();
            objectOutput.writeObject(object);
            return objectOutput.getCopyOfWrittenBuffer();
        } catch (Exception e) {
            throw new SerializerException("Can not serialize object " + object, e);
        }
    }

    @Override
    public Object deserialize(byte[] bytes) {
        FSTObjectInput objectInput = OBJECTINPUT.get();
        try {
            objectInput.resetForReuseUseArray(bytes);
            return objectInput.readObject();
        } catch (Exception e) {
            throw new SerializerException("Can not deserialize from bytes with " + CONFIGURATION, e);
        }
    }
}
