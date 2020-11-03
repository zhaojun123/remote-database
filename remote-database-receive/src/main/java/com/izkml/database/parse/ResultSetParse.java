package com.izkml.database.parse;

import com.izkml.database.result.Field;
import com.izkml.database.result.ResultWrapper;
import com.izkml.database.utils.Base64Utils;
import com.izkml.database.utils.StringUtils;

import java.io.UnsupportedEncodingException;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 分析ResultSet
 */
public class ResultSetParse {

    private ResultSet resultSet;

    private ResultSetMetaData rsmd;

    public ResultSetParse(ResultSet resultSet) throws SQLException {
        this.resultSet = resultSet;
        this.rsmd = resultSet.getMetaData();
    }

    public ResultWrapper parse() throws SQLException {
        List<Field> fieldList = setField();
        List<List<Object>> results = new ArrayList<>();
        while (resultSet.next()) {
            results.add(getResultValue());
        }
        ResultWrapper resultWrapper = new ResultWrapper();
        resultWrapper.setFields(fieldList);
        resultWrapper.setResults(results);
        return resultWrapper;
    }

    private List<Field> setField() throws SQLException {
        List<Field> fieldList = new ArrayList<>();
        for(int i=0;i<rsmd.getColumnCount();i++){
            int index = i+1;
            Field field = new Field();
            field.setAutoIncrement(rsmd.isAutoIncrement(index));
            field.setCaseSensitive(rsmd.isCaseSensitive(index));
            field.setCatalogName(rsmd.getCatalogName(index));
            field.setColumnClassName(rsmd.getColumnClassName(index));
            field.setColumnDisplaySize(rsmd.getColumnDisplaySize(index));
            field.setColumnLabel(rsmd.getColumnLabel(index));
            field.setColumnName(rsmd.getColumnName(index));
            field.setColumnType(rsmd.getColumnType(index));
            field.setColumnTypeName(rsmd.getColumnTypeName(index));
            field.setCurrency(rsmd.isCurrency(index));
            field.setDefinitelyWritable(rsmd.isDefinitelyWritable(index));
            field.setNullable(rsmd.isNullable(index));
            field.setPrecision(rsmd.getPrecision(index));
            field.setReadOnly(rsmd.isReadOnly(index));
            field.setScale(rsmd.getScale(index));
            field.setSchemaName(rsmd.getSchemaName(index));
            field.setSigned(rsmd.isSigned(index));
            field.setTableName(rsmd.getTableName(index));
            field.setWritable(rsmd.isWritable(index));
            fieldList.add(field);
        }
        return fieldList;
    }

    private List<Object> getResultValue() throws SQLException {
        List<Object> list = new ArrayList<>();
        for(int i=0;i<rsmd.getColumnCount();i++){
            int index = i+1;
            int sqlType = rsmd.getColumnType(index);
            Object value = null;
            switch (sqlType) {
                case Types.NUMERIC:
                case Types.DECIMAL:
                    value = resultSet.getBigDecimal(index);
                    break;
                case Types.BIT:
                    value = resultSet.getBoolean(index);
                    break;

                case Types.TINYINT:
                    value = resultSet.getByte(index);
                    break;

                case Types.SMALLINT:
                    value = resultSet.getShort(index);
                    break;

                case Types.INTEGER:
                    value = resultSet.getInt(index);
                    break;

                case Types.BIGINT:
                    value = resultSet.getLong(index);
                    break;

                case Types.REAL:
                    value = resultSet.getFloat(index);
                    break;

                case Types.FLOAT:
                case Types.DOUBLE:
                    value = resultSet.getDouble(index);
                    break;

                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    //因为json不支持传输byte[] 所以需要将值用base64序列化
                    value = resultSet.getBytes(index);
                    try {
                        value = Base64Utils.encode((byte[]) value);
                    } catch (UnsupportedEncodingException e) {
                        e.printStackTrace();
                    }
                    break;
                case Types.DATE:
                    value = StringUtils.safeToString(resultSet.getDate(index));
                    break;

                case Types.TIME:
                    value = StringUtils.safeToString(resultSet.getTime(index));
                    break;

                case Types.TIMESTAMP:
                    value = StringUtils.safeToString(resultSet.getTimestamp(index));
                    break;

                case Types.BLOB:
                    Blob blob  = resultSet.getBlob(index);
                    if(blob !=null){
                        try {
                            value = Base64Utils.encode((blob.getBytes(1,(int)blob.length())));
                        } catch (UnsupportedEncodingException e) {
                            e.printStackTrace();
                        }
                    }
                    break;
                case Types.CLOB:
                    Clob clob  = resultSet.getClob(index);
                    if(clob !=null){
                        value = clob.getSubString(1,(int)clob.length());
                    }
                    break;
                default:
                    value = resultSet.getObject(index);
            }
            list.add(value);
        }
        return list;
    }
}
