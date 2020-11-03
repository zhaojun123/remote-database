package com.izkml.database.jdbc.utils;

import java.sql.SQLException;

public class SQLExceptionUtils {

    public static SQLException METHOD_NOT_SUPPORT() throws SQLException {
        return new SQLException("the method not support");
    }

}
