package com.izkml.database.utils;

import java.io.*;

/**
 * io 工具类
 */
public class IOUtils {

    /**
     * inputStream 转换成 byte[]
     * @param input
     * @return
     * @throws IOException
     */
    public static byte[] toByteArray(InputStream input) throws IOException {
        if(input == null){
            return null;
        }
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int n = 0;
        while (-1 != (n = input.read(buffer))) {
            output.write(buffer, 0, n);
        }
        return output.toByteArray();
    }

    /**
     * bytes 转换成 inputStream
     * @param bytes
     * @return
     */
    public static InputStream toInputStream(byte[] bytes){
        if(bytes == null){
            return null;
        }
        return new ByteArrayInputStream(bytes);
    }

    /**
     * bytes转换成Reader
     * @param bytes
     * @return
     */
    public static Reader toReader(byte[] bytes){
        if(bytes == null){
            return null;
        }
        return new InputStreamReader(toInputStream(bytes));
    }

    public static String readerToString(Reader reader) throws IOException {
        if(reader == null){
            return null;
        }
        char[]c = new char[4096];
        int len = 0;

        StringBuilder buf = new StringBuilder();

        while ((len = reader.read(c)) != -1) {
            buf.append(c, 0, len);
        }
        return buf.toString();
    }

}
