package com.izkml.database.jdbc;

import com.izkml.database.jdbc.result.Field;
import com.izkml.database.jdbc.result.ResultWrapper;
import com.izkml.database.jdbc.utils.UnWrapUtils;

import java.sql.SQLException;

public class ResultSetMetaDataImpl implements java.sql.ResultSetMetaData{

    private ResultWrapper resultWrapper;

    public ResultSetMetaDataImpl(ResultWrapper resultWrapper){
        this.resultWrapper = resultWrapper;
    }

    private void checkBound(int columnIndex) throws SQLException{
        if(resultWrapper == null || columnIndex> resultWrapper.getFields().size()){
            throw new SQLException("ResultSet.Column index out of range");
        }
    }

    private Field findField(int columnIndex) throws SQLException{
        checkBound(columnIndex);
        return resultWrapper.getFields().get(columnIndex-1);
    }

    @Override
    public int getColumnCount() throws SQLException {
        if(resultWrapper == null){
            return 0;
        }
        return resultWrapper.getFields().size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkBound(column);
        return findField(column).getAutoIncrement();
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return findField(column).getCaseSensitive();
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return true;
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return findField(column).getCurrency();
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return findField(column).getNullable();
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return findField(column).getSigned();
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return findField(column).getColumnDisplaySize();
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return findField(column).getColumnLabel();
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return findField(column).getColumnName();
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return findField(column).getSchemaName();
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return findField(column).getPrecision();
    }

    @Override
    public int getScale(int column) throws SQLException {
        return findField(column).getScale();
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return findField(column).getTableName();
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return findField(column).getCatalogName();
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return findField(column).getColumnType();
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return findField(column).getColumnTypeName();
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return findField(column).getReadOnly();
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return findField(column).getWritable();
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return findField(column).getDefinitelyWritable();
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return findField(column).getColumnClassName();
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
