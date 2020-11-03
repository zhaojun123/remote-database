package com.izkml.database.jdbc;

import com.izkml.database.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.sql.Savepoint;

public class SavepointImpl implements Savepoint {

    private String savepointName ;

    public SavepointImpl(){
        this(null);
    }

    public SavepointImpl(String name){
        if(StringUtils.isEmpty(name)){
            name = StringUtils.getUniqueId();
        }
        this.savepointName = name;
    }

    @Override
    public int getSavepointId() throws SQLException {
        return 0;
    }

    @Override
    public String getSavepointName() throws SQLException {
        return savepointName;
    }
}
