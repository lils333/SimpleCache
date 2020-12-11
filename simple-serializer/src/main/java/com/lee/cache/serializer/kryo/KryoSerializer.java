package com.lee.cache.serializer.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.lee.cache.exception.SerializerException;
import com.lee.cache.serializer.BaseSerAndDeser;
import lombok.extern.slf4j.Slf4j;
import org.objenesis.strategy.StdInstantiatorStrategy;

/**
 * @author l46li
 */
@Slf4j
public class KryoSerializer<S> extends BaseSerAndDeser<S> {

    private static final byte[] EMPTY = new byte[0];

    private static final ThreadLocal<Kryo> KRYOTHREADLOCAL = ThreadLocal.withInitial(() -> {
        Kryo kryo = new Kryo();
        //这个是一个工具类，如果为了性能考虑，请单独自己实现序列化器com.esotericsoftware.kryo.Serializer
        //然后注册到kryo里面去，这个地方了为了尽量通用，也就是可以序列化任何类型，所以需要关闭setRegistrationRequired
        //并且不能够使用register注册功能，这样kryo自己去推断类型
        kryo.setRegistrationRequired(false);
        //确定没有嵌套引用，那么就关闭它，有一定的新能提升
        kryo.setReferences(false);
        //如果我们序列化的类没有默认的无参数构造器，那么就是用该方式
        kryo.setInstantiatorStrategy(new StdInstantiatorStrategy());
        return kryo;
    });

    /**
     * 注意设置成 -1 是为了自动扩展,也就是当1024个字节不够用了，那么会自动扩张，而不会报错
     */
    private static final ThreadLocal<Output> OUTPUTTHREADLOCAL = ThreadLocal.withInitial(() -> new Output(1024, -1));

    /**
     * 其实可以不指定大小的，反正只是用这个对象去包装byte数组而已
     */
    private static final ThreadLocal<Input> INPUTTHREADLOCAL = ThreadLocal.withInitial(Input::new);

    public KryoSerializer(Class<S> clazz) {
        super(clazz);
    }

    /**
     * 在写入的时候最好还是把class的信息写入进去，除非是自定的Serializer，那么可以使用writeObject
     *
     * @param obj 需要序列化的对象
     * @return 返回序列化后的字节数组
     */
    @Override
    public byte[] serialize(S obj) {
        Kryo kryo = KRYOTHREADLOCAL.get();
        Output output = OUTPUTTHREADLOCAL.get();
        try {
            //序列化会把对象的类型信息都写入到序列化字节数组里面去
            kryo.writeClassAndObject(output, obj);
            output.flush();
            return output.toBytes();
        } catch (Exception e) {
            throw new SerializerException("Can not serialize object : " + obj, e);
        } finally {
            output.reset();
        }
    }

    /**
     * 需要反序列化的字节数组，也会读取class的类型信息
     *
     * @param bytes 需要反序列化的字节数组
     * @return 需要的泛型对象
     */
    @Override
    @SuppressWarnings("unchecked")
    public S deserialize(byte[] bytes) {
        Kryo kryo = KRYOTHREADLOCAL.get();
        Input input = INPUTTHREADLOCAL.get();
        try {
            input.setBuffer(bytes);
            //因为序列化已经把需要的类型信息写入到该字节数组了，所以直接转就可以了
            return (S) kryo.readClassAndObject(input);
        } catch (Exception e) {
            throw new SerializerException("Can not deserialize from bytes with kryo : " + kryo, e);
        } finally {
            input.setBuffer(EMPTY);
        }
    }
}
