package com.izkml.database.jdbc.json;

import com.alibaba.fastjson.JSON;

import java.lang.reflect.Type;

public class FastJsonImpl implements Json{


    @Override
    public String toJson(Object ob) {
        return JSON.toJSONString(ob);
    }

    @Override
    public <T> T parse(String json, Class<T> clazz) {
        return JSON.parseObject(json,clazz);
    }

    @Override
    public <T> T parse(String json, Type type) {
        return JSON.parseObject(json,type);
    }
}
