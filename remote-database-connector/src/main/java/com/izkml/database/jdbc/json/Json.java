package com.izkml.database.jdbc.json;

import java.lang.reflect.Type;

/**
 * json工具接口
 */
public interface Json {

    /**
     * 对象转换成json
     * @param ob
     * @return
     */
    String toJson(Object ob);

    /**
     * json转换成对象
     * @param json
     * @param clazz
     * @param <T>
     * @return
     */
    <T> T parse(String json,Class<T> clazz);

    /**
     * json转换成对象 复杂对象
     * @param json
     * @param type
     * @param <T>
     * @return
     */
    <T> T parse(String json, Type type);
}
