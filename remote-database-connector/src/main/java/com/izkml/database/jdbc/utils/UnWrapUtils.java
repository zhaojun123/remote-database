package com.izkml.database.jdbc.utils;

import java.sql.SQLException;

public class UnWrapUtils {

    public static  <T> T unwrap(Class<T> iface,Object ob) throws SQLException {
        try {
            // This works for classes that aren't actually wrapping anything
            return iface.cast(ob);
        } catch (ClassCastException cce) {
            throw new SQLException("Unable to unwrap to " + iface.toString());
        }
    }

    public static boolean isWrapperFor(Class<?> iface,Object ob) throws SQLException {
        return iface.isInstance(ob);
    }

}
