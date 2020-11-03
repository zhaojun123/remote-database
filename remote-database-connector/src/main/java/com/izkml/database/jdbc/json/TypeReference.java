package com.izkml.database.jdbc.json;

import java.lang.reflect.Type;

public class TypeReference<T> extends com.fasterxml.jackson.core.type.TypeReference<T>{

    private Type _type;

    public TypeReference(){}

    public TypeReference(Type type){
        this._type = type;
    }

    @Override
    public Type getType() {
        return _type;
    }

}
