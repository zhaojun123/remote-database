package com.izkml.database.jdbc.json;

import java.lang.reflect.Type;

public class JsonUtils{

    private static Json delegate;

    static {
        delegate = JsonFactory.createJson();
    }

    /**
     * 设置自定义json
     * @param json
     */
    public static void setJson(Json json){
        JsonUtils.delegate = json;
    }

    public static String toJson(Object ob) {
        check();
        return delegate.toJson(ob);
    }

    public static  <T> T parse(String json, Class<T> clazz) {
        check();
        return delegate.parse(json,clazz);
    }

    public static  <T> T parse(String json, Type type) {
        check();
        return delegate.parse(json,type);
    }

    private static void check(){
        if(delegate == null){
            throw new JsonException("not find com.database.jdbc.json.Json implement，please use com.database.jdbc.json.JsonUtils.setJson ");
        }
    }
}
