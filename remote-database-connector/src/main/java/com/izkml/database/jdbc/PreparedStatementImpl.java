package com.izkml.database.jdbc;

import com.izkml.database.jdbc.param.JdbcType;
import com.izkml.database.jdbc.param.Param;
import com.izkml.database.jdbc.param.ParamWrapper;
import com.izkml.database.jdbc.param.StatementType;
import com.izkml.database.jdbc.parse.SqlParse;
import com.izkml.database.jdbc.parse.SqlType;
import com.izkml.database.jdbc.result.ResultWrapper;
import com.izkml.database.jdbc.utils.Base64Utils;
import com.izkml.database.jdbc.utils.IOUtils;
import com.izkml.database.jdbc.utils.SQLExceptionUtils;
import com.izkml.database.jdbc.web.WebUtils;
import com.izkml.database.jdbc.utils.UnWrapUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.net.URL;
import java.sql.*;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;

public class PreparedStatementImpl extends StatementImpl implements java.sql.PreparedStatement{

    private ParamWrapper currentParamWrapper;

    private List<ParamWrapper> batchList = new ArrayList<>();


    public PreparedStatementImpl(String sql,ConnectionImpl connection) throws SQLException {
        super(connection);
        currentParamWrapper = new ParamWrapper(connection,sql, StatementType.PREPAREDSTATEMENT);
    }

    @Override
    public ResultSet executeQuery() throws SQLException {
        checkClosed();
        ResultWrapper resultWrapper = WebUtils.doPostTemplate(getConnection().getWebUrl()+"/"+Constant.QUERY,currentParamWrapper, ResultWrapper.class);
        ResultSetImpl resultSet =  new ResultSetImpl(getConnection(),this,resultWrapper);
        setResultSet(resultSet);
        return resultSet;
    }

    @Override
    public int executeUpdate() throws SQLException {
        checkClosed();
        setResultSet(null);
        int updateCount = WebUtils.doPostTemplate(getConnection().getWebUrl()+"/"+Constant.UPDATE,currentParamWrapper, Integer.class);
        setUpdateCount(updateCount);
        return updateCount;
    }

    private void setValue(int parameterIndex, Object value, JdbcType jdbcType) throws SQLException {
        checkClosed();
        if(value != null){
            try {
                if(jdbcType == JdbcType.BYTES){
                    value = Base64Utils.encode((byte[])value);
                }else if(jdbcType == JdbcType.DATE
                        || jdbcType == JdbcType.TIME
                        || jdbcType == JdbcType.TIMESTAMP
                        || jdbcType == JdbcType.URL){
                    value = value.toString();
                }else if(jdbcType == JdbcType.INPUTSTREAM){
                    value = Base64Utils.encode((InputStream)value);
                }else if(jdbcType == JdbcType.READER){
                    value = IOUtils.readerToString((Reader) value);
                }else if(jdbcType == JdbcType.BLOB){
                    Blob blob = (Blob)value;
                    value = Base64Utils.encode(blob.getBytes(1, (int) blob.length()));
                }else if(jdbcType == JdbcType.CLOB){
                    Clob clob = (Clob)value;
                    value = clob.getSubString(1,(int)clob.length());
                }
            } catch (IOException e) {
                throw new SQLException("ResultSet.ColumnIndex "+ parameterIndex +" decode fail");
            }
        }
        Param param = new Param(parameterIndex,value,jdbcType);
        currentParamWrapper.getParamList().add(param);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType) throws SQLException {
        setValue(parameterIndex,null,JdbcType.ISNULL);
    }

    @Override
    public void setBoolean(int parameterIndex, boolean x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.BOOLEAN);
    }

    @Override
    public void setByte(int parameterIndex, byte x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.BYTE);
    }

    @Override
    public void setShort(int parameterIndex, short x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.SHORT);
    }

    @Override
    public void setInt(int parameterIndex, int x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.INTEGER);
    }

    @Override
    public void setLong(int parameterIndex, long x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.LONG);
    }

    @Override
    public void setFloat(int parameterIndex, float x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.FLOAT);
    }

    @Override
    public void setDouble(int parameterIndex, double x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.DOUBLE);
    }

    @Override
    public void setBigDecimal(int parameterIndex, BigDecimal x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.BIGDECIMAL);
    }

    @Override
    public void setString(int parameterIndex, String x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.STRING);
    }

    @Override
    public void setBytes(int parameterIndex, byte[] x) throws SQLException {
        setValue(parameterIndex, x,JdbcType.BYTES);
    }

    @Override
    public void setDate(int parameterIndex, Date x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.DATE);
    }

    @Override
    public void setTime(int parameterIndex, Time x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.TIME);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.TIMESTAMP);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setAsciiStream(parameterIndex,x);
    }

    @Override
    public void setUnicodeStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex,x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, int length) throws SQLException {
        setBinaryStream(parameterIndex,x);
    }

    @Override
    public void clearParameters() throws SQLException {
        checkClosed();
        currentParamWrapper.getParamList().clear();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType) throws SQLException {
        setObject(parameterIndex,x);
    }

    @Override
    public void setObject(int parameterIndex, Object parameterObj) throws SQLException {
        if (parameterObj == null) {
            setNull(parameterIndex,-1);
        } else {
            if (parameterObj instanceof Byte) {
                setInt(parameterIndex, ((Byte) parameterObj).intValue());

            } else if (parameterObj instanceof String) {
                setString(parameterIndex, (String) parameterObj);

            } else if (parameterObj instanceof BigDecimal) {
                setBigDecimal(parameterIndex, (BigDecimal) parameterObj);

            } else if (parameterObj instanceof Short) {
                setShort(parameterIndex, ((Short) parameterObj).shortValue());

            } else if (parameterObj instanceof Integer) {
                setInt(parameterIndex, ((Integer) parameterObj).intValue());

            } else if (parameterObj instanceof Long) {
                setLong(parameterIndex, ((Long) parameterObj).longValue());

            } else if (parameterObj instanceof Float) {
                setFloat(parameterIndex, ((Float) parameterObj).floatValue());

            } else if (parameterObj instanceof Double) {
                setDouble(parameterIndex, ((Double) parameterObj).doubleValue());

            } else if (parameterObj instanceof byte[]) {
                setBytes(parameterIndex, (byte[]) parameterObj);

            } else if (parameterObj instanceof java.sql.Date) {
                setDate(parameterIndex, (java.sql.Date) parameterObj);

            } else if (parameterObj instanceof Time) {
                setTime(parameterIndex, (Time) parameterObj);

            } else if (parameterObj instanceof Timestamp) {
                setTimestamp(parameterIndex, (Timestamp) parameterObj);

            } else if (parameterObj instanceof Boolean) {
                setBoolean(parameterIndex, ((Boolean) parameterObj).booleanValue());

            } else if (parameterObj instanceof InputStream) {
                setBinaryStream(parameterIndex, (InputStream) parameterObj, -1);

            } else if (parameterObj instanceof java.sql.Blob) {
                setBlob(parameterIndex, (java.sql.Blob) parameterObj);

            } else if (parameterObj instanceof java.sql.Clob) {
                setClob(parameterIndex, (java.sql.Clob) parameterObj);

            } else if (parameterObj instanceof java.util.Date) {
                setTimestamp(parameterIndex, new Timestamp(((java.util.Date) parameterObj).getTime()));

            } else if (parameterObj instanceof BigInteger) {
                setString(parameterIndex, parameterObj.toString());

            } else if (parameterObj instanceof LocalDate) {
                setDate(parameterIndex, Date.valueOf((LocalDate) parameterObj));

            } else if (parameterObj instanceof LocalDateTime) {
                setTimestamp(parameterIndex, Timestamp.valueOf((LocalDateTime) parameterObj));

            } else if (parameterObj instanceof LocalTime) {
                setTime(parameterIndex, Time.valueOf((LocalTime) parameterObj));

            } else {
                setString(parameterIndex, parameterObj.toString());
            }
        }
    }

    @Override
    public boolean execute() throws SQLException {
        checkClosed();
        SqlParse parse = new SqlParse(currentParamWrapper.getSql());
        parse.parse();
        if(parse.getSqlType()== SqlType.SELECT){
            executeQuery();
        }else{
            executeUpdate();
        }
        return true;
    }

    @Override
    public void addBatch() throws SQLException {
        batchList.add(currentParamWrapper);
        currentParamWrapper = new ParamWrapper(getConnection(),currentParamWrapper.getSql(), StatementType.PREPAREDSTATEMENT);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, int length) throws SQLException {
        setCharacterStream(parameterIndex,reader);
    }

    @Override
    public void setRef(int parameterIndex, Ref x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void setBlob(int parameterIndex, Blob x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.BLOB);
    }

    @Override
    public void setClob(int parameterIndex, Clob x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.CLOB);
    }

    @Override
    public void setArray(int parameterIndex, Array x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public ResultSetMetaData getMetaData() throws SQLException {
        return getResultSet()== null?null:getResultSet().getMetaData();
    }

    @Override
    public void setDate(int parameterIndex, Date x, Calendar cal) throws SQLException {
        setDate(parameterIndex,x);
    }

    @Override
    public void setTime(int parameterIndex, Time x, Calendar cal) throws SQLException {
        setTime(parameterIndex,x);
    }

    @Override
    public void setTimestamp(int parameterIndex, Timestamp x, Calendar cal) throws SQLException {
        setTimestamp(parameterIndex,x);
    }

    @Override
    public void setNull(int parameterIndex, int sqlType, String typeName) throws SQLException {
        setNull(parameterIndex,sqlType);
    }

    @Override
    public void setURL(int parameterIndex, URL x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.URL);
    }

    @Override
    public ParameterMetaData getParameterMetaData() throws SQLException {
        return null;
    }

    @Override
    public void setRowId(int parameterIndex, RowId x) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void setNString(int parameterIndex, String value) throws SQLException {
        setString(parameterIndex,value);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value, long length) throws SQLException {
        setNCharacterStream(parameterIndex,value);
    }

    @Override
    public void setNClob(int parameterIndex, NClob value) throws SQLException {
        setClob(parameterIndex,value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex,reader);
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream, long length) throws SQLException {
        setBlob(parameterIndex,inputStream);
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader, long length) throws SQLException {
        setClob(parameterIndex,reader);
    }

    @Override
    public void setSQLXML(int parameterIndex, SQLXML xmlObject) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength) throws SQLException {
        setObject(parameterIndex,x);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setAsciiStream(parameterIndex,x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x, long length) throws SQLException {
        setBinaryStream(parameterIndex,x);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader, long length) throws SQLException {
        setCharacterStream(parameterIndex,reader);
    }

    @Override
    public void setAsciiStream(int parameterIndex, InputStream x) throws SQLException {
        setBinaryStream(parameterIndex,x);
    }

    @Override
    public void setBinaryStream(int parameterIndex, InputStream x) throws SQLException {
        setValue(parameterIndex,x,JdbcType.INPUTSTREAM);
    }

    @Override
    public void setCharacterStream(int parameterIndex, Reader reader) throws SQLException {
        setValue(parameterIndex,reader,JdbcType.READER);
    }

    @Override
    public void setNCharacterStream(int parameterIndex, Reader value) throws SQLException {
        setCharacterStream(parameterIndex,value);
    }

    @Override
    public void setClob(int parameterIndex, Reader reader) throws SQLException {
        try {
            setClob(parameterIndex,new com.izkml.database.jdbc.io.Clob(IOUtils.readerToString(reader)));
        } catch (IOException e) {
            throw new SQLException("ResultSet.ColumnIndex "+ parameterIndex +" set fail",e);
        }
    }

    @Override
    public void setBlob(int parameterIndex, InputStream inputStream) throws SQLException {
        try {
            setBlob(parameterIndex,new com.izkml.database.jdbc.io.Blob(IOUtils.toByteArray(inputStream)));
        } catch (IOException e) {
            throw new SQLException("ResultSet.ColumnIndex "+ parameterIndex +" set fail",e);
        }
    }

    @Override
    public void setNClob(int parameterIndex, Reader reader) throws SQLException {
        setClob(parameterIndex,reader);
    }


    @Override
    public void close() throws SQLException {
        super.close();
        currentParamWrapper = null;
        batchList = null;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public void clearBatch() throws SQLException {
        batchList.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return super.executeBatch();
    }

    @Override
    public ConnectionImpl getConnection() throws SQLException {
        return super.getConnection();
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
