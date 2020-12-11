package com.lee.cache.serializer.protostuff;

import io.protostuff.Exclude;
import io.protostuff.Tag;
import io.protostuff.runtime.IdStrategy;
import io.protostuff.runtime.RuntimeEnv;
import io.protostuff.runtime.RuntimeFieldFactory;
import io.protostuff.runtime.RuntimeSchema;

import java.lang.reflect.Modifier;
import java.util.*;

import static io.protostuff.runtime.RuntimeEnv.ID_STRATEGY;

/**
 * 代理了一下RuntimeSchema 的静态创建，主要是去除掉transient字段不序列化的地方,其他代码都是从RuntimeSchema 拷贝过来
 */
public class SchemaFactory {

    public static final int MIN_TAG_VALUE = 1;
    // 2^29 - 1
    public static final int MAX_TAG_VALUE = 536870911;

    private static final Set<String> NO_EXCLUSIONS = Collections.emptySet();

    public static final String ERROR_TAG_VALUE = "Invalid tag number (value must be in range [1, 2^29-1])";

    /**
     * Generates a schema from the given class.
     * <p>
     * Method overload for backwards compatibility.
     */
    public static <T> RuntimeSchema<T> createFrom(Class<T> typeClass) {
        return createFrom(typeClass, NO_EXCLUSIONS, ID_STRATEGY);
    }

    /**
     * Generates a schema from the given class with the exclusion of certain fields.
     * 注意：如果序列化的时候字段上面有@Deprecated，那么序列化的时候还是会序列化的，只是反序列化的时候不反序列化该字段而已
     * 所以该字段对应的tag值还是不变,也就是序列化的时候tag的值还是写进去
     * <p>
     * 如果想要和protobuf 兼用：那么字段的序列号就很重要了
     * ProtobufIOUtil来序列化和反序列化
     * 默认使用字段出现在类里面的值作为序号，也可以使用@Tag来指定，但是只要使用了@Tag其他字段也必须使用@Tag标注
     * <p>
     * 如果有循环引用：
     * 那么请使用GraphIOUtil来序列化和反序列化
     */
    public static <T> RuntimeSchema<T> createFrom(Class<T> typeClass, Set<String> exclusions, IdStrategy strategy) {
        if (typeClass.isInterface() || Modifier.isAbstract(typeClass.getModifiers())) {
            throw new RuntimeException(
                    "The root object can neither be an abstract "
                            + "class nor interface: \"" + typeClass.getName());
        }

        final Map<String, java.lang.reflect.Field> fieldMap = findInstanceFields(typeClass);
        final ArrayList<io.protostuff.runtime.Field<T>> fields = new ArrayList<>(fieldMap.size());
        int i = 0;
        boolean annotated = false;
        for (java.lang.reflect.Field f : fieldMap.values()) {
            if (!exclusions.contains(f.getName())) {
                if (f.getAnnotation(Deprecated.class) != null) {
                    // this field should be ignored by ProtoStuff.
                    // preserve its field number for backward-forward compat
                    i++;
                    continue;
                }

                final Tag tag = f.getAnnotation(Tag.class);
                final int fieldMapping;
                final String name;
                if (tag == null) {
                    // Fields gets assigned mapping tags according to their
                    // definition order
                    if (annotated) {
                        String className = typeClass.getCanonicalName();
                        String fieldName = f.getName();
                        String message = String.format("%s#%s is not annotated with @Tag", className, fieldName);
                        throw new RuntimeException(message);
                    }
                    fieldMapping = ++i;

                    name = f.getName();
                } else {
                    // Fields gets assigned mapping tags according to their
                    // annotation
                    if (!annotated && !fields.isEmpty()) {
                        throw new RuntimeException(
                                "When using annotation-based mapping, "
                                        + "all fields must be annotated with @"
                                        + Tag.class.getSimpleName());
                    }
                    annotated = true;
                    fieldMapping = tag.value();

                    if (fieldMapping < MIN_TAG_VALUE || fieldMapping > MAX_TAG_VALUE) {
                        throw new IllegalArgumentException(ERROR_TAG_VALUE + ": " + fieldMapping + " on " + typeClass);
                    }

                    name = tag.alias().isEmpty() ? f.getName() : tag.alias();
                }

                final io.protostuff.runtime.Field<T> field
                        = RuntimeFieldFactory.getFieldFactory(f.getType(), strategy).create(fieldMapping, name, f, strategy);
                fields.add(field);
            }
        }

        return new RuntimeSchema<>(typeClass, fields, RuntimeEnv.newInstantiator(typeClass));
    }

    private static Map<String, java.lang.reflect.Field> findInstanceFields(Class<?> typeClass) {
        LinkedHashMap<String, java.lang.reflect.Field> fieldMap = new LinkedHashMap<>();
        fill(fieldMap, typeClass);
        return fieldMap;
    }

    private static void fill(Map<String, java.lang.reflect.Field> fieldMap, Class<?> typeClass) {
        if (Object.class != typeClass.getSuperclass()) {
            fill(fieldMap, typeClass.getSuperclass());
        }

        for (java.lang.reflect.Field f : typeClass.getDeclaredFields()) {
            int mod = f.getModifiers();
//            if (!Modifier.isStatic(mod) && !Modifier.isTransient(mod) && f.getAnnotation(Exclude.class) == null) {
//                fieldMap.put(f.getName(), f);
//            }
            //标注了@Exclude 和 transient 字段直接忽律掉，为了让transient也可以被序列化，那么这个地方忽律掉transient字段
            if (!Modifier.isStatic(mod) && f.getAnnotation(Exclude.class) == null) {
                fieldMap.put(f.getName(), f);
            }
        }
    }
}
