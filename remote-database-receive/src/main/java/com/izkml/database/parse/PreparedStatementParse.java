package com.izkml.database.parse;

import com.izkml.database.param.Param;
import com.izkml.database.param.ParamWrapper;
import com.izkml.database.utils.Base64Utils;
import com.izkml.database.utils.StringUtils;

import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.*;
import java.util.List;

public class PreparedStatementParse {

    private PreparedStatement statement;
    private ParamWrapper paramWrapper;

    public PreparedStatementParse(PreparedStatement preparedStatement, ParamWrapper paramWrapper){
        this.statement =preparedStatement;
        this.paramWrapper = paramWrapper;
    }

    public void parse() throws SQLException {
        setParams();
    }

    private void setParams() throws SQLException {
        if(paramWrapper == null || paramWrapper.getParamList().isEmpty()){
            return;
        }
        List<Param> list = paramWrapper.getParamList();
        if(list==null || list.isEmpty()){
            return;
        }
        for(Param param:list){
            int index = param.getParameterIndex();
            Object value = param.getValue();
            if(value == null){
                statement.setObject(index,null);
                continue;
            }
            switch (param.getJdbcType()){
                case URL:
                    try {
                        statement.setURL(index,new URL(value.toString()));
                    } catch (MalformedURLException e) {
                        throw new SQLException("param index "+index+" value "+value+" not a URL");
                    }
                    break;
                case BLOB:
                case BYTES:
                case INPUTSTREAM:
                    try {
                        byte[] bytes = Base64Utils.decode(value.toString());
                        statement.setBytes(index,bytes);
                    } catch (UnsupportedEncodingException e) {
                        throw new SQLException("param index "+index+" UnsupportedEncoding",e);
                    }
                    break;
                case BYTE:
                case INTEGER:
                    statement.setInt(index,Integer.parseInt(value.toString()));
                    break;
                case CLOB:
                    StringReader stringReader = new StringReader(value.toString());
                    statement.setCharacterStream(index,stringReader);
                    break;
                case DATE:
                    statement.setDate(index,Date.valueOf(value.toString()));
                    break;
                case LONG:
                    statement.setLong(index,Long.parseLong(value.toString()));
                    break;
                case TIME:
                    statement.setTime(index, Time.valueOf(value.toString()));
                    break;
                case FLOAT:
                    statement.setFloat(index,Float.parseFloat(value.toString()));
                    break;
                case SHORT:
                    statement.setShort(index,Short.parseShort(value.toString()));
                    break;
                case DOUBLE:
                    statement.setDouble(index,Double.parseDouble(value.toString()));
                    break;
                case READER:
                    statement.setCharacterStream(index,new StringReader(value.toString()));
                    break;
                case STRING:
                    statement.setString(index,value.toString());
                    break;
                case BOOLEAN:
                    statement.setBoolean(index,Boolean.parseBoolean(value.toString()));
                    break;
                case TIMESTAMP:
                    statement.setTimestamp(index,Timestamp.valueOf(value.toString()));
                    break;
                case BIGDECIMAL:
                    statement.setBigDecimal(index,new BigDecimal(value.toString()));
                    break;
                default:
                    statement.setObject(index,value);
            }
        }
    }
}
