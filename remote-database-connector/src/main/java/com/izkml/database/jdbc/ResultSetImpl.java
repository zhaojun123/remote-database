package com.izkml.database.jdbc;

import com.izkml.database.jdbc.result.Field;
import com.izkml.database.jdbc.result.ResultWrapper;
import com.izkml.database.jdbc.utils.Base64Utils;
import com.izkml.database.jdbc.utils.SQLExceptionUtils;
import com.izkml.database.jdbc.utils.StringUtils;
import com.izkml.database.jdbc.utils.UnWrapUtils;

import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;

public class ResultSetImpl implements java.sql.ResultSet {

    private ResultWrapper resultWrapper;

    private ConnectionImpl connection;

    private Statement statement;

    private ResultSetMetaData resultSetMetaData;

    /**
     * 存储字段名称和对应下标
     */
    private Map<String,Integer> columnNamesCache = new HashMap<>();

    /**
     * 互斥锁
     */
    private Object mutex = new Object();

    public ResultSetImpl(ConnectionImpl connection,Statement statement,ResultWrapper resultWrapper){
        this.connection = connection;
        this.statement = statement;
        this.resultWrapper = resultWrapper;
        this.resultSetMetaData = new ResultSetMetaDataImpl(resultWrapper);
    }

    /**
     *  ResultSet 游标位置
     */
    private volatile int index = -1;

    /**
     * ResultSet是否被关闭
     */
    private volatile boolean close = false;

    private void checkClosed() throws SQLException {
        if(close){
            throw new SQLException("ResultSet.Operation not allowed after ResultSet closed");
        }
    }

    private void checkColumnBound(int columnIndex) throws SQLException{
        if(resultWrapper == null || columnIndex> resultWrapper.getFields().size()){
            throw new SQLException("ResultSet.Column index out of range");
        }
    }

    private void checkRowBound(int index) throws SQLException{
        if(wasRowEmpty() || index> resultWrapper.getResults().size()-1){
            throw new SQLException("ResultSet.Column index out of range");
        }
    }

    private boolean wasRowEmpty(){
        return resultWrapper == null?true:resultWrapper.getResults().isEmpty()?true:false;
    }

    private Object getValueByColumnIndex(int columnIndex) throws SQLException {
        checkClosed();
        checkColumnBound(columnIndex);
        checkRowBound(index);
        return resultWrapper.getResults().get(index).get(columnIndex-1);
    }

    @Override
    public boolean next() throws SQLException {
        checkClosed();
        if(resultWrapper == null){
            return false;
        }
        synchronized (mutex){
            index++;
            return resultWrapper.getResults().size()>index;
        }
    }

    @Override
    public void close() throws SQLException {
        if(connection == null){
            return;
        }
        synchronized (mutex){
            close = true;
            index = -1;
            connection = null;
            statement = null;
            resultWrapper = null;
        }
    }

    @Override
    public boolean wasNull() throws SQLException {
        checkClosed();
        return false;
    }

    @Override
    public String getString(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        return String.valueOf(value);
    }

    @Override
    public boolean getBoolean(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return false;
        }
        if(value instanceof Boolean){
            return (boolean)value;
        }
        return Boolean.valueOf(value.toString());
    }

    @Override
    public byte getByte(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return 0;
        }
        if(value instanceof Byte){
            return (byte)value;
        }
        return Byte.parseByte(value.toString());
    }

    @Override
    public short getShort(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return 0;
        }
        if(value instanceof Short){
            return (short)value;
        }
        return Short.parseShort(value.toString());
    }

    @Override
    public int getInt(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return 0;
        }
        if(value instanceof Integer){
            return (int)value;
        }
        return Integer.parseInt(value.toString());
    }

    @Override
    public long getLong(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return 0;
        }
        if(value instanceof Long){
            return (long)value;
        }
        return Long.parseLong(value.toString());
    }

    @Override
    public float getFloat(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return 0;
        }
        if(value instanceof Float){
            return (float)value;
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public double getDouble(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return 0;
        }
        if(value instanceof Float){
            return (float)value;
        }
        return Float.parseFloat(value.toString());
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex, int scale) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        if(value instanceof BigDecimal){
            return (BigDecimal)value;
        }
        return new BigDecimal(value.toString()).setScale(scale);
    }

    /**
     * 因为 json无法传输byte[]，所以需要将byte[]转换成base64
     * 反序列化也是先将base64转换成byte[]
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    @Override
    public byte[] getBytes(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        if(value instanceof byte[]){
            return (byte[])value;
        }
        try {
            return Base64Utils.decode(value.toString());
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("ResultSet.Column "+ resultWrapper.getFields().get(columnIndex).getColumnLabel()+" decode fail",e);
        }
    }

    @Override
    public Date getDate(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        if(value instanceof Date){
            return (Date)value;
        }
        return Date.valueOf(value.toString());
    }

    @Override
    public Time getTime(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        if(value instanceof Time){
            return (Time)value;
        }
        return Time.valueOf(value.toString());
    }

    @Override
    public Timestamp getTimestamp(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        if(value instanceof Timestamp){
            return (Timestamp)value;
        }
        return Timestamp.valueOf(value.toString());
    }

    /**
     * 因为 json无法传输byte[]，所以需要将byte[]转换成base64
     * 反序列化也是先将base64转换成byte[]
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    @Override
    public InputStream getAsciiStream(int columnIndex) throws SQLException {
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getUnicodeStream(int columnIndex) throws SQLException {
        return getBinaryStream(columnIndex);
    }

    @Override
    public InputStream getBinaryStream(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        if(value instanceof InputStream){
            return (InputStream)value;
        }
        try {
            return Base64Utils.encodeInputStream(value.toString());
        } catch (UnsupportedEncodingException e) {
            throw new SQLException("ResultSet.Column "+ resultWrapper.getFields().get(columnIndex).getColumnLabel()+" decode fail",e);
        }
    }

    @Override
    public String getString(String columnLabel) throws SQLException {
        return getString(findColumn(columnLabel));
    }

    @Override
    public boolean getBoolean(String columnLabel) throws SQLException {
        return getBoolean(findColumn(columnLabel));
    }

    @Override
    public byte getByte(String columnLabel) throws SQLException {
        return getByte(findColumn(columnLabel));
    }

    @Override
    public short getShort(String columnLabel) throws SQLException {
        return getShort(findColumn(columnLabel));
    }

    @Override
    public int getInt(String columnLabel) throws SQLException {
        return getInt(findColumn(columnLabel));
    }

    @Override
    public long getLong(String columnLabel) throws SQLException {
        return getLong(findColumn(columnLabel));
    }

    @Override
    public float getFloat(String columnLabel) throws SQLException {
        return getFloat(findColumn(columnLabel));
    }

    @Override
    public double getDouble(String columnLabel) throws SQLException {
        return getDouble(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel, int scale) throws SQLException {
        return getBigDecimal(findColumn(columnLabel),scale);
    }

    @Override
    public byte[] getBytes(String columnLabel) throws SQLException {
        return getBytes(findColumn(columnLabel));
    }

    @Override
    public Date getDate(String columnLabel) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(String columnLabel) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(String columnLabel) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public InputStream getAsciiStream(String columnLabel) throws SQLException {
        return getAsciiStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getUnicodeStream(String columnLabel) throws SQLException {
        return getUnicodeStream(findColumn(columnLabel));
    }

    @Override
    public InputStream getBinaryStream(String columnLabel) throws SQLException {
        return getBinaryStream(findColumn(columnLabel));
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public String getCursorName() throws SQLException {
        return null;
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return resultSetMetaData;
    }

    @Override
    public Object getObject(int columnIndex) throws SQLException {

        switch (getMetaData().getColumnType(columnIndex)) {
            case Types.BIT:
            case Types.BOOLEAN:
                return Boolean.valueOf(getBoolean(columnIndex));

            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
                return Integer.valueOf(getInt(columnIndex));
            case Types.BIGINT:
                return Long.valueOf(getLong(columnIndex));
            case Types.DECIMAL:
            case Types.NUMERIC:
                return getBigDecimal(columnIndex);
            case Types.REAL:
                return new Float(getFloat(columnIndex));
            case Types.FLOAT:
            case Types.DOUBLE:
                return new Double(getDouble(columnIndex));
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                return getString(columnIndex);
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                return getBytes(columnIndex);
            case Types.DATE:
                return getDate(columnIndex);
            case Types.TIME:
                return getTime(columnIndex);
            case Types.TIMESTAMP:
                return getTimestamp(columnIndex);
            default:
                return getString(columnIndex);
        }
    }

    @Override
    public Object getObject(String columnLabel) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public int findColumn(String columnLabel) throws SQLException {
        if(StringUtils.isEmpty(columnLabel)){
            throw new SQLException("ResultSet.Column is not allow empty");
        }
        if(columnNamesCache.containsKey(columnLabel)){
            return columnNamesCache.get(columnLabel);
        }
        int index = 1;
        for(Field field:resultWrapper.getFields()){
            if(field.getColumnLabel().equalsIgnoreCase(columnLabel)){
                columnNamesCache.put(columnLabel,index);
                return index;
            }
            index++;
        }
        throw new SQLException("ResultSet.Column "+columnLabel+" is not find");
    }

    /**
     * Reader 从String 获取
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    @Override
    public Reader getCharacterStream(int columnIndex) throws SQLException {
        return new StringReader(getString(columnIndex));
    }

    @Override
    public Reader getCharacterStream(String columnLabel) throws SQLException {
        return getCharacterStream(findColumn(columnLabel));
    }

    @Override
    public BigDecimal getBigDecimal(int columnIndex) throws SQLException {
        Object value = getValueByColumnIndex(columnIndex);
        if(value == null){
            return null;
        }
        if(value instanceof BigDecimal){
            return (BigDecimal)value;
        }
        return new BigDecimal(value.toString());
    }

    @Override
    public BigDecimal getBigDecimal(String columnLabel) throws SQLException {
        return getBigDecimal(findColumn(columnLabel));
    }

    @Override
    public boolean isBeforeFirst() throws SQLException {
        checkClosed();
        if(!wasRowEmpty() && index==-1){
            return true;
        }
        return false;
    }

    @Override
    public boolean isAfterLast() throws SQLException {
        checkClosed();
        if(!wasRowEmpty() && index > resultWrapper.getResults().size()-1){
            return true;
        }
        return false;
    }

    @Override
    public boolean isFirst() throws SQLException {
        checkClosed();
        if(!wasRowEmpty() && index==1){
            return true;
        }
        return false;
    }

    @Override
    public boolean isLast() throws SQLException {
        checkClosed();
        if(!wasRowEmpty() && index == resultWrapper.getResults().size()-1){
            return true;
        }
        return false;
    }

    @Override
    public void beforeFirst() throws SQLException {

    }

    @Override
    public void afterLast() throws SQLException {

    }

    @Override
    public boolean first() throws SQLException {
        checkClosed();
        synchronized (mutex){
            if(!wasRowEmpty()){
                index = -1;
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean last() throws SQLException {
        checkClosed();
        synchronized (mutex){
            if(!wasRowEmpty()){
                index = resultWrapper.getResults().size()-1;
                return true;
            }
            return false;
        }
    }

    @Override
    public int getRow() throws SQLException {
        checkClosed();
        if(!wasRowEmpty()){
            return index+1;
        }
        return 0;
    }

    @Override
    public boolean absolute(int row) throws SQLException {
        checkClosed();
        synchronized (mutex){
            if(!wasRowEmpty()){
                checkRowBound(row);
                index = row;
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean relative(int rows) throws SQLException {
        checkClosed();
        synchronized (mutex){
            if(!wasRowEmpty()){
                checkRowBound(rows+index);
                index += rows;
                return true;
            }
            return false;
        }
    }

    @Override
    public boolean previous() throws SQLException {
        return relative(-1);
    }

    /**
     * 控制方向 暂不支持
     * ResultSet.FETCH_FORWARD  ResultSet.FETCH_REVERSE ResultSet.FETCH_UNKNOWN
     * @param direction
     * @throws SQLException
     */
    @Override
    public void setFetchDirection(int direction) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    /**
     * ResultSet.TYPE_SCROLL_INSENSITIVE
     * ResultSet.TYPE_FORWARD_ONLY
     * ResultSet.TYPE_SCROLL_SENSITIVE
     * @return
     * @throws SQLException
     */
    @Override
    public int getType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    /**
     * 并发性
     * ResultSet.CONCUR_READ_ONLY
     * ResultSet.CONCUR_UPDATABLE
     * @return
     * @throws SQLException
     */
    @Override
    public int getConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public boolean rowUpdated() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public boolean rowInserted() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public boolean rowDeleted() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNull(int columnIndex) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBoolean(int columnIndex, boolean x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateByte(int columnIndex, byte x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateShort(int columnIndex, short x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateInt(int columnIndex, int x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateLong(int columnIndex, long x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateFloat(int columnIndex, float x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateDouble(int columnIndex, double x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBigDecimal(int columnIndex, BigDecimal x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateString(int columnIndex, String x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBytes(int columnIndex, byte[] x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateDate(int columnIndex, Date x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateTime(int columnIndex, Time x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateTimestamp(int columnIndex, Timestamp x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, int length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, int length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateObject(int columnIndex, Object x, int scaleOrLength) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateObject(int columnIndex, Object x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNull(String columnLabel) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBoolean(String columnLabel, boolean x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateByte(String columnLabel, byte x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateShort(String columnLabel, short x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateInt(String columnLabel, int x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateLong(String columnLabel, long x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateFloat(String columnLabel, float x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateDouble(String columnLabel, double x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBigDecimal(String columnLabel, BigDecimal x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateString(String columnLabel, String x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBytes(String columnLabel, byte[] x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateDate(String columnLabel, Date x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateTime(String columnLabel, Time x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateTimestamp(String columnLabel, Timestamp x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, int length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, int length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateObject(String columnLabel, Object x, int scaleOrLength) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateObject(String columnLabel, Object x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void insertRow() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateRow() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void deleteRow() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void refreshRow() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void cancelRowUpdates() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void moveToInsertRow() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void moveToCurrentRow() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public Statement getStatement() throws SQLException {
        return statement;
    }

    @Override
    public Object getObject(int columnIndex, Map<String, Class<?>> map) throws SQLException {
        return getObject(columnIndex);
    }

    @Override
    public Ref getRef(int columnIndex) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public Blob getBlob(int columnIndex) throws SQLException {
        byte[] bytes = getBytes(columnIndex);
        if(bytes!=null){
            return new com.izkml.database.jdbc.io.Blob(bytes);
        }
        return null;
    }

    /**
     * 这里当作string处理
     * @param columnIndex
     * @return
     * @throws SQLException
     */
    @Override
    public Clob getClob(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return str==null?null:new com.izkml.database.jdbc.io.Clob(str);
    }

    @Override
    public Array getArray(int columnIndex) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public Object getObject(String columnLabel, Map<String, Class<?>> map) throws SQLException {
        return getObject(findColumn(columnLabel));
    }

    @Override
    public Ref getRef(String columnLabel) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public Blob getBlob(String columnLabel) throws SQLException {
        return getBlob(findColumn(columnLabel));
    }

    @Override
    public Clob getClob(String columnLabel) throws SQLException {
        return getClob(findColumn(columnLabel));
    }

    @Override
    public Array getArray(String columnLabel) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public Date getDate(int columnIndex, Calendar cal) throws SQLException {
        return getDate(columnIndex);
    }

    @Override
    public Date getDate(String columnLabel, Calendar cal) throws SQLException {
        return getDate(findColumn(columnLabel));
    }

    @Override
    public Time getTime(int columnIndex, Calendar cal) throws SQLException {
        return getTime(columnIndex);
    }

    @Override
    public Time getTime(String columnLabel, Calendar cal) throws SQLException {
        return getTime(findColumn(columnLabel));
    }

    @Override
    public Timestamp getTimestamp(int columnIndex, Calendar cal) throws SQLException {
        return getTimestamp(columnIndex);
    }

    @Override
    public Timestamp getTimestamp(String columnLabel, Calendar cal) throws SQLException {
        return getTimestamp(findColumn(columnLabel));
    }

    @Override
    public URL getURL(int columnIndex) throws SQLException {
        String val = getString(columnIndex);

        if (val == null) {
            return null;
        }

        try {
            return new URL(val);
        } catch (MalformedURLException mfe) {
            throw new SQLException("Malformed URL "+val);
        }
    }

    @Override
    public URL getURL(String columnLabel) throws SQLException {
        return getURL(findColumn(columnLabel));
    }

    @Override
    public void updateRef(int columnIndex, Ref x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateRef(String columnLabel, Ref x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBlob(int columnIndex, java.sql.Blob x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBlob(String columnLabel, java.sql.Blob x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateClob(int columnIndex, java.sql.Clob x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateClob(String columnLabel, java.sql.Clob x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }


    @Override
    public void updateArray(int columnIndex, Array x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateArray(String columnLabel, Array x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public RowId getRowId(int columnIndex) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public RowId getRowId(String columnLabel) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateRowId(int columnIndex, RowId x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateRowId(String columnLabel, RowId x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public int getHoldability() throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return close;
    }

    @Override
    public void updateNString(int columnIndex, String nString) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNString(String columnLabel, String nString) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNClob(int columnIndex, java.sql.NClob nClob) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNClob(String columnLabel, java.sql.NClob nClob) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }


    @Override
    public java.sql.NClob getNClob(int columnIndex) throws SQLException {
        String str = getString(columnIndex);
        return str == null ? null : new com.izkml.database.jdbc.io.NClob(str);
    }

    @Override
    public java.sql.NClob getNClob(String columnLabel) throws SQLException {
        return getNClob(findColumn(columnLabel));
    }

    @Override
    public SQLXML getSQLXML(int columnIndex) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public SQLXML getSQLXML(String columnLabel) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateSQLXML(int columnIndex, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateSQLXML(String columnLabel, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public String getNString(int columnIndex) throws SQLException {
        return getString(columnIndex);
    }

    @Override
    public String getNString(String columnLabel) throws SQLException {
        return getNString(findColumn(columnLabel));
    }

    @Override
    public Reader getNCharacterStream(int columnIndex) throws SQLException {
        return getCharacterStream(columnIndex);
    }

    @Override
    public Reader getNCharacterStream(String columnLabel) throws SQLException {
        return getNCharacterStream(findColumn(columnLabel));
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader, long length) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateAsciiStream(int columnIndex, InputStream x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBinaryStream(int columnIndex, InputStream x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateCharacterStream(int columnIndex, Reader x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateAsciiStream(String columnLabel, InputStream x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBinaryStream(String columnLabel, InputStream x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateCharacterStream(String columnLabel, Reader reader) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBlob(int columnIndex, InputStream inputStream) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateBlob(String columnLabel, InputStream inputStream) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateClob(String columnLabel, Reader reader) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNClob(int columnIndex, Reader reader) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateNClob(String columnLabel, Reader reader) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public <T> T getObject(int columnIndex, Class<T> type) throws SQLException {
        if (type == null) {
            throw new SQLException("Type parameter can not be null");
        }
        if (type.equals(String.class)) {
            return (T) getString(columnIndex);
        } else if (type.equals(BigDecimal.class)) {
            return (T) getBigDecimal(columnIndex);
        } else if (type.equals(Boolean.class) || type.equals(Boolean.TYPE)) {
            return (T) Boolean.valueOf(getBoolean(columnIndex));
        } else if (type.equals(Integer.class) || type.equals(Integer.TYPE)) {
            return (T) Integer.valueOf(getInt(columnIndex));
        } else if (type.equals(Long.class) || type.equals(Long.TYPE)) {
            return (T) Long.valueOf(getLong(columnIndex));
        } else if (type.equals(Float.class) || type.equals(Float.TYPE)) {
            return (T) Float.valueOf(getFloat(columnIndex));
        } else if (type.equals(Double.class) || type.equals(Double.TYPE)) {
            return (T) Double.valueOf(getDouble(columnIndex));
        } else if (type.equals(byte[].class)) {
            return (T) getBytes(columnIndex);
        } else if (type.equals(java.sql.Date.class)) {
            return (T) getDate(columnIndex);
        } else if (type.equals(Time.class)) {
            return (T) getTime(columnIndex);
        } else if (type.equals(Timestamp.class)) {
            return (T) getTimestamp(columnIndex);
        } else if (type.equals(com.izkml.database.jdbc.io.Clob.class)) {
            return (T) getClob(columnIndex);
        } else if (type.equals(com.izkml.database.jdbc.io.Blob.class)) {
            return (T) getBlob(columnIndex);
        } else if (type.equals(Array.class)) {
            return (T) getArray(columnIndex);
        } else if (type.equals(Ref.class)) {
            return (T) getRef(columnIndex);
        } else if (type.equals(URL.class)) {
            return (T) getURL(columnIndex);
        } else {
            return type.cast(getObject(columnIndex));
        }
    }

    @Override
    public <T> T getObject(String columnLabel, Class<T> type) throws SQLException {
        return getObject(findColumn(columnLabel),type);
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType, int scaleOrLength) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateObject(int columnIndex, Object x, SQLType targetSqlType) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void updateObject(String columnLabel, Object x, SQLType targetSqlType) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return UnWrapUtils.unwrap(iface,this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return UnWrapUtils.isWrapperFor(iface,this);
    }
}
