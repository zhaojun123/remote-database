package com.izkml.database.utils;

public class StringUtils {

    public static String safeToString(Object value){
        if(value!=null){
            return value.toString();
        }
        return null;
    }

}
