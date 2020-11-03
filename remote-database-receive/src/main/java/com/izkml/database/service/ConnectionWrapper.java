package com.izkml.database.service;

import java.sql.Connection;

public class ConnectionWrapper {

    private Connection connection;

    private Long lastTime;

    public ConnectionWrapper(Connection connection){
        this.connection = connection;
        lastTime = System.currentTimeMillis();
    }

    public void refresh(){
        lastTime = System.currentTimeMillis();
    }

    public Connection getConnection() {
        return connection;
    }

    public void setConnection(Connection connection) {
        this.connection = connection;
    }

    public Long getLastTime() {
        return lastTime;
    }

    public void setLastTime(Long lastTime) {
        this.lastTime = lastTime;
    }
}
