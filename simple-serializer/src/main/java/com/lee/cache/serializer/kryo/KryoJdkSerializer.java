package com.lee.cache.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.serializers.BeanSerializer;
import com.esotericsoftware.kryo.serializers.JavaSerializer;
import com.lee.cache.exception.SerializerException;
import com.lee.cache.serializer.BaseSerAndDeser;
import lombok.extern.slf4j.Slf4j;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * 因为在写得时候已经把所有的类的信息都一起写入了，所以不需要在为它提供任何的类型相关的信息，它能够很好的反序列化
 * 但是这个kryo不能够序列化瞬态字段，所以这个也是需要考虑的，如果我们有瞬态字段需要序列化的时候
 *
 * @author l46li
 */
@Slf4j
public class KryoJdkSerializer extends BaseSerAndDeser<Object> {

    private static final byte[] EMPTY = new byte[0];

    private final ThreadLocal<Kryo> KRYO = new ThreadLocal<Kryo>() {
        @Override
        protected Kryo initialValue() {
            Kryo kryo = new Kryo();
            kryo.setRegistrationRequired(false);
            kryo.setReferences(false);
            kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
            if (isCompatibleJdk) {
                kryo.setDefaultSerializer(JavaSerializer.class);
            } else {
                kryo.setDefaultSerializer(BeanSerializer.class);
            }
            return kryo;
        }
    };

    private static final ThreadLocal<Output> OUTPUT = ThreadLocal.withInitial(() -> new Output(1024, -1));

    private static final ThreadLocal<Input> INPUT = ThreadLocal.withInitial(Input::new);

    private final boolean isCompatibleJdk;

    public KryoJdkSerializer(boolean isCompatibleJdk) {
        this.isCompatibleJdk = isCompatibleJdk;
    }

    @Override
    public byte[] serialize(Object obj) {
        Kryo kryo = KRYO.get();
        Output output = OUTPUT.get();
        try {
            kryo.writeClassAndObject(output, obj);
            output.flush();
            return output.toBytes();
        } catch (Exception e) {
            throw new SerializerException("Can not serialize object : " + obj, e);
        } finally {
            output.reset();
        }
    }

    @Override
    public Object deserialize(byte[] bytes) {
        Kryo kryo = KRYO.get();
        Input input = INPUT.get();
        try {
            input.setBuffer(bytes);
            return kryo.readClassAndObject(input);
        } catch (Exception e) {
            throw new SerializerException("Can not deserialize from bytes with kryo " + kryo, e);
        } finally {
            input.setBuffer(EMPTY);
        }
    }
}
