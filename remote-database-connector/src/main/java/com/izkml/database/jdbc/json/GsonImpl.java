package com.izkml.database.jdbc.json;

import com.google.gson.Gson;

import java.lang.reflect.Type;

public class GsonImpl implements Json{

    private final Gson gson = new Gson();

    @Override
    public String toJson(Object ob) {
        return gson.toJson(ob);
    }

    @Override
    public <T> T parse(String json, Class<T> clazz) {
        return gson.fromJson(json,clazz);
    }

    @Override
    public <T> T parse(String json, Type type) {
        return gson.fromJson(json,type);
    }
}
