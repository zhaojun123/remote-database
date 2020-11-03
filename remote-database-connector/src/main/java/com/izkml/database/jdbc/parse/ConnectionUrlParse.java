package com.izkml.database.jdbc.parse;

import com.izkml.database.jdbc.ConnectionImpl;
import com.izkml.database.jdbc.utils.StringUtils;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Connection url 解析
 */
public class ConnectionUrlParse {

    private static final Pattern CONNECTION_STRING_PTRN = Pattern.compile("\\s*(jdbc:)(?<databaseProductName>[\\w]+)\\s*"
            +"(:)(?<protocol>[^?:#]+)\\s*"
            +"(://)(?<host>[^?:#]+)"
            +"(:)(?<port>[^?/#]+)"
            +"(/)?(\\s*)(?<schema>[^?#]*)?"
            +"(?:\\?(?!\\s*\\?)(?<query>[^#]*))?"
           );

    private static final Pattern PROPERTIES_PTRN = Pattern.compile("[&\\s]*(?<key>[\\w\\.\\-\\s%]*)(?:=(?<value>[^&]*))?");

    private ConnectionImpl connection;

    private String connectionUrl;

    public ConnectionUrlParse(ConnectionImpl connection,String connectionUrl)throws SQLException {
        if(StringUtils.isEmpty(connectionUrl)){
            throw new SQLException("connection url is not empty");
        }
        this.connection = connection;
        this.connectionUrl = connectionUrl;
    }

    public static boolean valid(String connectionUrl){
        Matcher matcher = CONNECTION_STRING_PTRN.matcher(connectionUrl);
        return matcher.find();
    }

    /**
     * 解析connectionUrl
     * @throws SQLException
     */
    public void parse() throws SQLException {
        Matcher matcher = CONNECTION_STRING_PTRN.matcher(connectionUrl);
        if(!matcher.find()){
            throw new SQLException("connection url is illegal");
        }
        connection.setDatabaseProductName(matcher.group("databaseProductName"));
        connection.setProtocol(matcher.group("protocol"));
        connection.setHost(matcher.group("host"));
        connection.setPort(Integer.valueOf(matcher.group("port")));
        connection.setSchema(matcher.group("schema"));
        connection.setWebUrl(connection.getProtocol()+"://"+connection.getHost()+":"+connection.getPort());
        //将query的数据放入property内
        String query = matcher.group("query");
        if(!StringUtils.isEmpty(query)){
            Properties properties = connection.getClientInfo();
            if(properties == null){
                properties = new Properties();
                connection.setClientInfo(properties);
            }
            processKeyValuePattern(query,properties);
        }
    }

    private void processKeyValuePattern(String input,Properties properties) {
        Matcher matcher = PROPERTIES_PTRN.matcher(input);
        while (matcher.find()) {
            String key = matcher.group("key");
            String value = matcher.group("value");
            if (!StringUtils.isEmpty(key) && !StringUtils.isEmpty(value)) {
                properties.put(decode(key.trim()), decode(value.trim()));
            }
        }

    }

    private static String decode(String text) {
        if (StringUtils.isEmpty(text)) {
            return text;
        }
        try {
            return URLDecoder.decode(text, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            // Won't happen.
        }
        return "";
    }
}
