package com.izkml.database.jdbc.io;

public class NClob  extends Clob implements java.sql.NClob {

    public NClob(String charDataInit) {
        super(charDataInit);
    }
}
