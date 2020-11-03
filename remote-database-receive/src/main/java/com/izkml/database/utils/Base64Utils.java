package com.izkml.database.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Base64;

/**
 * base64工具
 */
public class Base64Utils {

    public final static String CHARSET = "UTF-8";

    public static String encode(byte[] bytes) throws UnsupportedEncodingException {
        if(bytes == null){
            return null;
        }
        return new String(Base64.getEncoder().encode(bytes),CHARSET);
    }

    public static byte[] decode(String text) throws UnsupportedEncodingException {
        if(text==null){
            return null;
        }
        return Base64.getDecoder().decode(text.getBytes(CHARSET));
    }

    public static String encode(InputStream inputStream) throws IOException {
        return encode(IOUtils.toByteArray(inputStream));
    }

    public static InputStream encodeInputStream(String text) throws UnsupportedEncodingException {
        return IOUtils.toInputStream(decode(text));
    }
}
