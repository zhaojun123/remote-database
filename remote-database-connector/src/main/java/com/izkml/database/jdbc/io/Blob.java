package com.izkml.database.jdbc.io;

import com.izkml.database.jdbc.utils.SQLExceptionUtils;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;

public class Blob implements java.sql.Blob, OutputStreamWatcher{

    //
    // This is a real brain-dead implementation of BLOB. Once I add streamability to the I/O for MySQL this will be more efficiently implemented
    // (except for the position() method, ugh).
    //

    /** The binary data that makes up this BLOB */
    private byte[] binaryData = null;
    private boolean isClosed = false;


    /**
     * Creates a BLOB encapsulating the given binary data
     *
     * @param data
     */
    public Blob(byte[] data) {
        setBinaryData(data);
    }


    private synchronized byte[] getBinaryData() {
        return this.binaryData;
    }

    /**
     * Retrieves the BLOB designated by this Blob instance as a stream.
     *
     * @return this BLOB represented as a binary stream of bytes.
     *
     * @throws SQLException
     *             if a database error occurs
     */
    @Override
    public synchronized java.io.InputStream getBinaryStream() throws SQLException {
        checkClosed();

        return new ByteArrayInputStream(getBinaryData());
    }

    /**
     * Returns as an array of bytes, part or all of the BLOB value that this
     * Blob object designates.
     *
     * @param pos
     *            where to start the part of the BLOB
     * @param length
     *            the length of the part of the BLOB you want returned.
     *
     * @return the bytes stored in the blob starting at position <code>pos</code> and having a length of <code>length</code>.
     *
     * @throws SQLException
     *             if a database error occurs
     */
    @Override
    public synchronized byte[] getBytes(long pos, int length) throws SQLException {
        checkClosed();

        if (pos < 1) {
            throw new SQLException("pos must larger than 0");
        }

        pos--;

        if (pos > this.binaryData.length) {
            throw new SQLException("\"pos\" argument can not be larger than the BLOB's length.");
        }

        if (pos + length > this.binaryData.length) {
            throw new SQLException("\"pos\" + \"length\" arguments can not be larger than the BLOB's length.");
        }

        byte[] newData = new byte[length];
        System.arraycopy(getBinaryData(), (int) (pos), newData, 0, length);

        return newData;
    }

    /**
     * Returns the number of bytes in the BLOB value designated by this Blob
     * object.
     *
     * @return the length of this blob
     *
     * @throws SQLException
     *             if a database error occurs
     */
    @Override
    public synchronized long length() throws SQLException {
        checkClosed();

        return getBinaryData().length;
    }

    /**
     * @see java.sql.Blob#position(byte[], long)
     */
    @Override
    public synchronized long position(byte[] pattern, long start) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    /**
     * Finds the position of the given pattern in this BLOB.
     *
     * @param pattern
     *            the pattern to find
     * @param start
     *            where to start finding the pattern
     *
     * @return the position where the pattern is found in the BLOB, -1 if not
     *         found
     *
     * @throws SQLException
     *             if a database error occurs
     */
    @Override
    public synchronized long position(java.sql.Blob pattern, long start) throws SQLException {
        checkClosed();

        return position(pattern.getBytes(0, (int) pattern.length()), start);
    }

    private synchronized void setBinaryData(byte[] newBinaryData) {
        this.binaryData = newBinaryData;
    }

    /**
     * @see Blob#setBinaryStream(long)
     */
    @Override
    public synchronized OutputStream setBinaryStream(long indexToWriteAt) throws SQLException {
        checkClosed();

        if (indexToWriteAt < 1) {
            throw new SQLException("indexToWriteAt must larger than 0");
        }

        WatchableOutputStream bytesOut = new WatchableOutputStream();
        bytesOut.setWatcher(this);

        if (indexToWriteAt > 0) {
            bytesOut.write(this.binaryData, 0, (int) (indexToWriteAt - 1));
        }

        return bytesOut;
    }

    /**
     * @see Blob#setBytes(long, byte[])
     */
    @Override
    public synchronized int setBytes(long writeAt, byte[] bytes) throws SQLException {
        checkClosed();

        return setBytes(writeAt, bytes, 0, bytes.length);
    }

    /**
     * @see Blob#setBytes(long, byte[], int, int)
     */
    @Override
    public synchronized int setBytes(long writeAt, byte[] bytes, int offset, int length) throws SQLException {
        checkClosed();

        OutputStream bytesOut = setBinaryStream(writeAt);

        try {
            bytesOut.write(bytes, offset, length);
        } catch (IOException ioEx) {
            throw new SQLException(ioEx);
        } finally {
            try {
                bytesOut.close();
            } catch (IOException doNothing) {
                // do nothing
            }
        }

        return length;
    }


    public synchronized void streamClosed(byte[] byteData) {
        this.binaryData = byteData;
    }


    @Override
    public synchronized void streamClosed(WatchableOutputStream out) {
        int streamSize = out.size();

        if (streamSize < this.binaryData.length) {
            out.write(this.binaryData, streamSize, this.binaryData.length - streamSize);
        }

        this.binaryData = out.toByteArray();
    }

    /**
     * Truncates the <code>BLOB</code> value that this <code>Blob</code> object represents to be <code>len</code> bytes in length.
     * <p>
     * <b>Note:</b> If the value specified for <code>len</code> is greater then the length+1 of the <code>BLOB</code> value then the behavior is undefined. Some
     * JDBC drivers may throw a <code>SQLException</code> while other drivers may support this operation.
     *
     * @param len
     *            the length, in bytes, to which the <code>BLOB</code> value
     *            that this <code>Blob</code> object represents should be truncated
     * @exception SQLException
     *                if there is an error accessing the <code>BLOB</code> value or if len is less than 0
     *                if the JDBC driver does not support
     *                this method
     * @since 1.4
     */
    @Override
    public synchronized void truncate(long len) throws SQLException {
        checkClosed();

        if (len < 0) {
            throw new SQLException("\"len\" argument can not be < 1.");
        }

        if (len > this.binaryData.length) {
            throw new SQLException("\"len\" argument can not be larger than the BLOB's length.");
        }

        // TODO: Do this without copying byte[]s by maintaining some end pointer on the original data

        byte[] newData = new byte[(int) len];
        System.arraycopy(getBinaryData(), 0, newData, 0, (int) len);
        this.binaryData = newData;
    }

    /**
     * This method frees the <code>Blob</code> object and releases the resources that
     * it holds. The object is invalid once the <code>free</code> method is called.
     * <p>
     * After <code>free</code> has been called, any attempt to invoke a method other than <code>free</code> will result in a <code>SQLException</code> being
     * thrown. If <code>free</code> is called multiple times, the subsequent calls to <code>free</code> are treated as a no-op.
     * <p>
     *
     * @throws SQLException
     *             if an error occurs releasing
     *             the Blob's resources
     *                if the JDBC driver does not support
     *                this method
     * @since 1.6
     */

    @Override
    public synchronized void free() throws SQLException {
        this.binaryData = null;
        this.isClosed = true;
    }

    /**
     * Returns an <code>InputStream</code> object that contains a partial <code>Blob</code> value,
     * starting with the byte specified by pos, which is length bytes in length.
     *
     * @param pos
     *            the offset to the first byte of the partial value to be retrieved.
     *            The first byte in the <code>Blob</code> is at position 1
     * @param length
     *            the length in bytes of the partial value to be retrieved
     * @return <code>InputStream</code> through which the partial <code>Blob</code> value can be read.
     * @throws SQLException
     *             if pos is less than 1 or if pos is greater than the number of bytes
     *             in the <code>Blob</code> or if pos + length is greater than the number of bytes
     *             in the <code>Blob</code>
     *
     *                if the JDBC driver does not support
     *                this method
     * @since 1.6
     */
    @Override
    public synchronized InputStream getBinaryStream(long pos, long length) throws SQLException {
        checkClosed();

        if (pos < 1) {
            throw new SQLException("\"pos\" argument can not be < 1.");
        }

        pos--;

        if (pos > this.binaryData.length) {
            throw new SQLException("\"pos\" argument can not be larger than the BLOB's length.");
        }

        if (pos + length > this.binaryData.length) {
            throw new SQLException("\"pos\" + \"length\" arguments can not be larger than the BLOB's length.");
        }

        return new ByteArrayInputStream(getBinaryData(), (int) pos, (int) length);
    }

    private synchronized void checkClosed() throws SQLException {
        if (this.isClosed) {
            throw new SQLException("Invalid operation on closed BLOB");
        }
    }

}
