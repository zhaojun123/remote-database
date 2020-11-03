package com.izkml.database.jdbc.parse;

import com.izkml.database.jdbc.utils.StringUtils;

import java.sql.SQLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlParse {

    private static Pattern NOTE = Pattern.compile("^/(\\*)*(.)*/");

    private SqlType sqlType;

    private String sql;

    public SqlType getSqlType(){
        return sqlType;
    }

    public SqlParse(String sql) throws SQLException {
        if(StringUtils.isEmpty(sql)){
            throw new SQLException("sql is not empty");
        }
        this.sql = sql;
    }

    public void parse(){
        if(sql.length()>=6){
            sql = sql.trim();
            //忽略注释
            if(sql.charAt(0) == '/' && sql.charAt(1) == '*'){
                sql = abortNotes(sql);
            }
            sql = sql.toLowerCase();
            if(sql.indexOf("select") == 0){
                sqlType = SqlType.SELECT;
            }else if(sql.indexOf("update") == 0){
                sqlType = SqlType.UPDATE;
            }else if(sql.indexOf("insert") == 0){
                sqlType = SqlType.INSERT;
            }else if(sql.indexOf("delete") == 0){
                sqlType = SqlType.DELETE;
            }else{
                sqlType = SqlType.UNKNOWN;
            }
        }else{
            sqlType = SqlType.UNKNOWN;
        }
    }

    /**
     * 忽略sql中的注释
     */
    public String abortNotes(String sql){
        Matcher matcher = NOTE.matcher(sql);
        if(matcher.find()){
            return abortNotes(matcher.replaceFirst(""));
        }
        return sql;
    }
}
