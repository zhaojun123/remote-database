package com.izkml.database.jdbc;

import com.izkml.database.jdbc.param.ParamWrapper;
import com.izkml.database.jdbc.parse.ConnectionUrlParse;
import com.izkml.database.jdbc.utils.StringUtils;
import com.izkml.database.jdbc.web.WebUtils;

import java.sql.*;
import java.util.Properties;
import java.util.logging.Logger;

public class Driver implements java.sql.Driver{

    static {
        try {
            DriverManager.registerDriver(new Driver());
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Connection connect(String url, Properties info) throws SQLException {
        String userName = info.getProperty("user");
        String password = info.getProperty("password");
        ConnectionImpl connection = new ConnectionImpl(url,userName,password,info);
        ConnectionUrlParse connectionUrlParse = new ConnectionUrlParse(connection,url);
        connectionUrlParse.parse();
        //ping
        RemotePing.regist(connection);
        //connect
        WebUtils.doPostTemplate(connection.getWebUrl()+"/"+Constant.CONNECT,new ParamWrapper(connection),null);
        return connection;
    }

    @Override
    public boolean acceptsURL(String url) throws SQLException {
        return ConnectionUrlParse.valid(url);
    }

    @Override
    public DriverPropertyInfo[] getPropertyInfo(String url, Properties info) throws SQLException {
        return new DriverPropertyInfo[0];
    }

    @Override
    public int getMajorVersion() {
        return 1;
    }

    @Override
    public int getMinorVersion() {
        return 0;
    }

    @Override
    public boolean jdbcCompliant() {
        return false;
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return null;
    }
}
