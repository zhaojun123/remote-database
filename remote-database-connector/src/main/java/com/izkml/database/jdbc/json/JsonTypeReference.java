package com.izkml.database.jdbc.json;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

/**
 * 用于json复杂对象序列化
 * @param <T>
 */
public abstract class JsonTypeReference<T> {

    protected final Type _type;

    protected JsonTypeReference()
    {
        Type superClass = getClass().getGenericSuperclass();
        _type = ((ParameterizedType) superClass).getActualTypeArguments()[0];
    }

    public Type getType() { return _type; }

}
