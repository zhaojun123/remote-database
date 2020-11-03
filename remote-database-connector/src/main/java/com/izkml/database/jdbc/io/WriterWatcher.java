package com.izkml.database.jdbc.io;

public interface WriterWatcher {

    /**
     * Called when the Writer being watched has .close() called
     */
    void writerClosed(WatchableWriter out);

}
