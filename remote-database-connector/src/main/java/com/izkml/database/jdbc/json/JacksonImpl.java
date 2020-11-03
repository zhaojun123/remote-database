package com.izkml.database.jdbc.json;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.IOException;
import java.lang.reflect.Type;

public class JacksonImpl implements Json{

    private final static ObjectMapper objectMapper = new ObjectMapper();

    static {
        // 忽略 在json字符串中存在，但是在java对象中不存在对应属性的情况，防止错误
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        // 忽略空bean转json的错误
        objectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS,false);
    }

    @Override
    public String toJson(Object ob){
        try {
            return objectMapper.writeValueAsString(ob);
        } catch (JsonProcessingException e) {
            throw new JsonException(e);
        }
    }

    @Override
    public <T> T parse(String json, Class<T> clazz) {
        try {
            return objectMapper.readValue(json,clazz);
        } catch (IOException e) {
            throw  new JsonException(e);
        }
    }

    @Override
    public <T> T parse(String json, Type type) {
        try {
            return objectMapper.readValue(json,new TypeReference(type));
        } catch (IOException e) {
            throw  new JsonException(e);
        }
    }
}
