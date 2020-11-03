package com.izkml.database.jdbc.io;

import java.io.*;
import java.sql.SQLException;

public class Clob implements java.sql.Clob, OutputStreamWatcher, WriterWatcher{

    private String charData;

    private String encode = "UTF-8";

    public Clob(String charDataInit) {
        this.charData = charDataInit;
    }

    /**
     * @see java.sql.Clob#getAsciiStream()
     */
    @Override
    public InputStream getAsciiStream() throws SQLException {
        if (this.charData != null) {
            try {
                return new ByteArrayInputStream(charData.getBytes(encode));
            } catch (UnsupportedEncodingException e) {

            }
        }
        return null;
    }

    /**
     * @see java.sql.Clob#getCharacterStream()
     */
    @Override
    public Reader getCharacterStream() throws SQLException {
        if (this.charData != null) {
            return new StringReader(this.charData);
        }

        return null;
    }

    /**
     * @see java.sql.Clob#getSubString(long, int)
     */
    @Override
    public String getSubString(long startPos, int length) throws SQLException {
        if (startPos < 1) {
            throw new SQLException("startPos must larger than 0");
        }

        int adjustedStartPos = (int) startPos - 1;
        int adjustedEndIndex = adjustedStartPos + length;

        if (this.charData != null) {
            if (adjustedEndIndex > this.charData.length()) {
                throw new SQLException("data length out of bounds");
            }

            return this.charData.substring(adjustedStartPos, adjustedEndIndex);
        }

        return null;
    }

    /**
     * @see java.sql.Clob#length()
     */
    @Override
    public long length() throws SQLException {
        if (this.charData != null) {
            return this.charData.length();
        }

        return 0;
    }

    /**
     * @see java.sql.Clob#position(Clob, long)
     */
    @Override
    public long position(java.sql.Clob arg0, long arg1) throws SQLException {
        return position(arg0.getSubString(1L, (int) arg0.length()), arg1);
    }

    /**
     * @see java.sql.Clob#position(String, long)
     */
    @Override
    public long position(String stringToFind, long startPos) throws SQLException {
        if (startPos < 1) {
            throw new SQLException("startPos must larger than 0");
        }

        if (this.charData != null) {
            if ((startPos - 1) > this.charData.length()) {
                throw new SQLException("data index out of bounds");
            }

            int pos = this.charData.indexOf(stringToFind, (int) (startPos - 1));

            return (pos == -1) ? (-1) : (pos + 1);
        }

        return -1;
    }

    /**
     * @see java.sql.Clob#setAsciiStream(long)
     */
    @Override
    public OutputStream setAsciiStream(long indexToWriteAt) throws SQLException {
        if (indexToWriteAt < 1) {
            throw new SQLException("indexToWriteAt must larger than 0");
        }

        WatchableOutputStream bytesOut = new WatchableOutputStream();
        bytesOut.setWatcher(this);

        if (indexToWriteAt > 0) {
            try {
                bytesOut.write(this.charData.getBytes(encode), 0, (int) (indexToWriteAt - 1));
            } catch (UnsupportedEncodingException e) {

            }
        }

        return bytesOut;
    }

    /**
     * @see java.sql.Clob#setCharacterStream(long)
     */
    @Override
    public Writer setCharacterStream(long indexToWriteAt) throws SQLException {
        if (indexToWriteAt < 1) {
            throw new SQLException("indexToWriteAt must larger than 0");
        }

        WatchableWriter writer = new WatchableWriter();
        writer.setWatcher(this);

        //
        // Don't call write() if nothing to write...
        //
        if (indexToWriteAt > 1) {
            writer.write(this.charData, 0, (int) (indexToWriteAt - 1));
        }

        return writer;
    }

    /**
     * @see java.sql.Clob#setString(long, String)
     */
    @Override
    public int setString(long pos, String str) throws SQLException {
        if (pos < 1) {
            throw new SQLException("pos must larger than 0");
        }

        if (str == null) {
            throw new SQLException("str must not null");
        }

        StringBuilder charBuf = new StringBuilder(this.charData);

        pos--;

        int strLength = str.length();

        charBuf.replace((int) pos, (int) (pos + strLength), str);

        this.charData = charBuf.toString();

        return strLength;
    }

    /**
     * @see java.sql.Clob#setString(long, String, int, int)
     */
    @Override
    public int setString(long pos, String str, int offset, int len) throws SQLException {
        if (pos < 1) {
            throw new SQLException("pos must larger than 0");
        }

        if (str == null) {
            throw new SQLException("str must not null");
        }

        StringBuilder charBuf = new StringBuilder(this.charData);

        pos--;

        try {
            String replaceString = str.substring(offset, offset + len);

            charBuf.replace((int) pos, (int) (pos + replaceString.length()), replaceString);
        } catch (StringIndexOutOfBoundsException e) {
            throw new SQLException(e);
        }

        this.charData = charBuf.toString();

        return len;
    }


    @Override
    public void streamClosed(WatchableOutputStream out) {
        int streamSize = out.size();

        if (streamSize < this.charData.length()) {
            try {
                out.write(this.charData.getBytes(encode), streamSize,
                        this.charData.length() - streamSize);
            } catch (UnsupportedEncodingException ex) {
                //
            }
        }

        try {
            this.charData = new String(out.toByteArray(),encode);
        } catch (UnsupportedEncodingException e) {

        }
    }

    /**
     * @see java.sql.Clob#truncate(long)
     */
    @Override
    public void truncate(long length) throws SQLException {
        if (length > this.charData.length()) {
            throw new SQLException("data index out of bounds");
        }

        this.charData = this.charData.substring(0, (int) length);
    }


    public void writerClosed(char[] charDataBeingWritten) {
        this.charData = new String(charDataBeingWritten);
    }


    @Override
    public void writerClosed(WatchableWriter out) {
        int dataLength = out.size();

        if (dataLength < this.charData.length()) {
            out.write(this.charData, dataLength, this.charData.length() - dataLength);
        }

        this.charData = out.toString();
    }

    @Override
    public void free() throws SQLException {
        this.charData = null;
    }

    @Override
    public Reader getCharacterStream(long pos, long length) throws SQLException {
        return new StringReader(getSubString(pos, (int) length));
    }

}
