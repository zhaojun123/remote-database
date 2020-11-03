package com.izkml.database.jdbc.io;

public interface OutputStreamWatcher {

    void streamClosed(WatchableOutputStream out);

}
