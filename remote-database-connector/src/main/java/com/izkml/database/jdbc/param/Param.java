package com.izkml.database.jdbc.param;


/**
 * 参数模型
 */
public class Param {

    /**
     * 字段值
     */
    private Object value;

    /**
     * 字段jdbc类型
     */
    private JdbcType jdbcType;

    /**
     * 字段值索引
     */
    private Integer parameterIndex;

    public Param(Integer parameterIndex,Object value,JdbcType jdbcType){
        this.parameterIndex = parameterIndex;
        this.value = value;
        this.jdbcType = jdbcType;
    }

    public Integer getParameterIndex() {
        return parameterIndex;
    }

    public void setParameterIndex(Integer parameterIndex) {
        this.parameterIndex = parameterIndex;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public JdbcType getJdbcType() {
        return jdbcType;
    }

    public void setJdbcType(JdbcType jdbcType) {
        this.jdbcType = jdbcType;
    }
}
