package com.izkml.database.jdbc.web;

import com.izkml.database.jdbc.json.JsonUtils;
import com.izkml.database.jdbc.json.ParameterizedTypeImpl;
import com.izkml.database.jdbc.utils.StringUtils;

import javax.net.ssl.*;
import java.io.*;
import java.lang.reflect.Type;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class WebUtils {

    private WebUtils() {

    }

    private static final String DEFAULT_CHARSET = "UTF-8";
    private static boolean ignoreSSLCheck = true; // 忽略SSL检查
    private static boolean ignoreHostCheck = true; // 忽略HOST检查

    private static final int connectTimeout = 30000;

    private static final int readTimeout = 0;

    public static void setIgnoreSSLCheck(boolean ignoreSSLCheck) {
        WebUtils.ignoreSSLCheck = ignoreSSLCheck;
    }

    public static void setIgnoreHostCheck(boolean ignoreHostCheck) {
        WebUtils.ignoreHostCheck = ignoreHostCheck;
    }

    public static class TrustAllTrustManager implements X509TrustManager {
        @Override
        public X509Certificate[] getAcceptedIssuers() {
            return null;
        }

        @Override
        public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }

        @Override
        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
        }
    }

    /**
     * http post请求 返回JsonResult
     * @param url
     * @param params
     * @param type
     * @param <T>
     * @return
     * @throws IOException
     */
    public static <T> T doPostTemplate(String url, Object params, Type type)throws  SQLException {
        return doPostTemplate(url,params,null,type);
    }

    /**
     * http post请求 返回JsonResult
     * @param url
     * @param params
     * @param headerMap
     * @param type
     * @param <T>
     * @return
     * @throws IOException
     */
    public static <T> T doPostTemplate(String url,Object params,Map<String, String> headerMap, Type type)throws  SQLException {
        return doPostTemplate(url,params,headerMap,connectTimeout,readTimeout,type);
    }

    public static <T> T doPostTemplate(String url, Object params, Map<String, String> headerMap, int connectTimeout, int readTimeout, Type type) throws SQLException {
        if(type == null){
            type = Object.class;
        }
        String result = doPost(url,params,DEFAULT_CHARSET,connectTimeout,readTimeout,headerMap);
        Type[] actualTypeArguments = new Type[]{type};
        ParameterizedTypeImpl parameterizedType = new ParameterizedTypeImpl(actualTypeArguments,null,JsonResult.class);
        JsonResult<T> jsonResult = JsonUtils.parse(result,parameterizedType);
        if(!jsonResult.getSuccess()){
            throw new SQLException("remote sql fail :"+jsonResult.getMsg());
        }
        return jsonResult.getData();
    }

    /**
     * 执行HTTP POST请求。
     *
     * @param url 请求地址
     * @param params 请求参数
     * @return 响应字符串
     */
    public static String doPost(String url, Object params, int connectTimeout, int readTimeout) throws SQLException {
        return doPost(url, params, DEFAULT_CHARSET, connectTimeout, readTimeout,null);
    }

    /**
     * 执行HTTP POST请求。带请求头
     * @param url
     * @param params
     * @param charset
     * @param connectTimeout
     * @param readTimeout
     * @param headerMap
     * @return
     * @throws IOException
     */
    public static String doPost(String url, Object params, String charset, int connectTimeout, int readTimeout, Map<String, String> headerMap) throws SQLException {
        String ctype = "application/json;charset=" + charset;
        byte[] content = {};
        try{
            if (params != null) {
                content = JsonUtils.toJson(params).getBytes(charset);
            }
            return _doPost(url, ctype, content, connectTimeout, readTimeout, headerMap);
        } catch (IOException e) {
            throw new SQLException("url:["+url+"] params:["+JsonUtils.toJson(params)+"] headerMap:["+headerMap+"]",e);
        }
    }

    private static String _doPost(String url, String ctype, byte[] content, int connectTimeout, int readTimeout, Map<String, String> headerMap) throws IOException {
        HttpURLConnection conn = null;
        OutputStream out = null;
        String rsp = null;
        try {
            conn = getConnection(new URL(url), "POST", ctype, headerMap);
            conn.setConnectTimeout(connectTimeout);
            conn.setReadTimeout(readTimeout);
            out = conn.getOutputStream();
            out.write(content);
            rsp = getResponseAsString(conn);
        } finally {
            if (out != null) {
                out.close();
            }
            if (conn != null) {
                conn.disconnect();
            }
        }

        return rsp;
    }

    private static HttpURLConnection getConnection(URL url, String method, String ctype, Map<String, String> headerMap) throws IOException {
        HttpURLConnection conn = (HttpURLConnection) url.openConnection();
        if (conn instanceof HttpsURLConnection) {
            HttpsURLConnection connHttps = (HttpsURLConnection) conn;
            if (ignoreSSLCheck) {
                try {
                    SSLContext ctx = SSLContext.getInstance("TLS");
                    ctx.init(null, new TrustManager[] { new TrustAllTrustManager() }, new SecureRandom());
                    connHttps.setSSLSocketFactory(ctx.getSocketFactory());
                    connHttps.setHostnameVerifier(new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession session) {
                            return true;
                        }
                    });
                } catch (Exception e) {
                    throw new IOException(e.toString());
                }
            } else {
                if (ignoreHostCheck) {
                    connHttps.setHostnameVerifier(new HostnameVerifier() {
                        @Override
                        public boolean verify(String hostname, SSLSession session) {
                            return true;
                        }
                    });
                }
            }
            conn = connHttps;
        }

        conn.setRequestMethod(method);
        conn.setDoInput(true);
        conn.setDoOutput(true);
        conn.setRequestProperty("Host", url.getHost());
        conn.setRequestProperty("Accept", "application/json, text/plain, */*");
        conn.setRequestProperty("Content-Type", ctype);
        if (headerMap != null) {
            for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                conn.setRequestProperty(entry.getKey(), entry.getValue());
            }
        }
        return conn;
    }

    protected static String getResponseAsString(HttpURLConnection conn) throws IOException {
        String charset = getResponseCharset(conn.getContentType());
        if (conn.getResponseCode() < HttpURLConnection.HTTP_BAD_REQUEST) {
            String contentEncoding = conn.getContentEncoding();
            if ("gzip".equalsIgnoreCase(contentEncoding)) {
                return getStreamAsString(new GZIPInputStream(conn.getInputStream()), charset);
            } else {
                return getStreamAsString(conn.getInputStream(), charset);
            }
        } else {
            // OAuth bad request always return 400 status
            if (conn.getResponseCode() == HttpURLConnection.HTTP_BAD_REQUEST) {
                InputStream error = conn.getErrorStream();
                if (error != null) {
                    return getStreamAsString(error, charset);
                }
            }
            // Client Error 4xx and Server Error 5xx
            throw new IOException(conn.getResponseCode() + " " + conn.getResponseMessage());
        }
    }

    public static String getStreamAsString(InputStream stream, String charset) throws IOException {
        try {
            Reader reader = new InputStreamReader(stream, charset);
            StringBuilder response = new StringBuilder();

            final char[] buff = new char[1024];
            int read = 0;
            while ((read = reader.read(buff)) > 0) {
                response.append(buff, 0, read);
            }

            return response.toString();
        } finally {
            if (stream != null) {
                stream.close();
            }
        }
    }

    public static String getResponseCharset(String ctype) {
        String charset = DEFAULT_CHARSET;

        if (!StringUtils.isEmpty(ctype)) {
            String[] params = ctype.split(";");
            for (String param : params) {
                param = param.trim();
                if (param.startsWith("charset")) {
                    String[] pair = param.split("=", 2);
                    if (pair.length == 2) {
                        if (!StringUtils.isEmpty(pair[1])) {
                            charset = pair[1].trim();
                        }
                    }
                    break;
                }
            }
        }

        return charset;
    }

}
