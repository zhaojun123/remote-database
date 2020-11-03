package com.izkml.database.jdbc.io;

import java.io.CharArrayWriter;

public class WatchableWriter extends CharArrayWriter {

    private WriterWatcher watcher;

    /**
     * @see java.io.Writer#close()
     */
    @Override
    public void close() {
        super.close();

        // Send data to watcher
        if (this.watcher != null) {
            this.watcher.writerClosed(this);
        }
    }

    /**
     * @param watcher
     */
    public void setWatcher(WriterWatcher watcher) {
        this.watcher = watcher;
    }

}
