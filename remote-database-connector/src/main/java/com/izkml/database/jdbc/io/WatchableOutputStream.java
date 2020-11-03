package com.izkml.database.jdbc.io;

import java.io.ByteArrayOutputStream;
import java.io.IOException;

public class WatchableOutputStream extends ByteArrayOutputStream {

    private OutputStreamWatcher watcher;

    /**
     * @see java.io.OutputStream#close()
     */
    @Override
    public void close() throws IOException {
        super.close();

        if (this.watcher != null) {
            this.watcher.streamClosed(this);
        }
    }

    /**
     * @param watcher
     */
    public void setWatcher(OutputStreamWatcher watcher) {
        this.watcher = watcher;
    }

}
