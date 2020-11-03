package com.izkml.database.param;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ParamWrapper {

    /**
     * 事务标示
     */
    @NotEmpty
    private String transcationId;

    /**
     * 是否自动提交
     */
    private Boolean autoCommit = true;

    /**
     * 事务级别
     */
    private Integer transactionIsolation;

    /**
     * 查询哪个库
     */
    private String schema;

    /**
     * 用户
     */
    private String userName;

    private String sql;

    private StatementType statementType;

    /**
     * 安全点 rollback的时候使用
     */
    private String savePointName;

    /**
     * sql的查询项参数
     */
    private List<Param> paramList = new ArrayList<>();

    public StatementType getStatementType() {
        return statementType;
    }

    public void setStatementType(StatementType statementType) {
        this.statementType = statementType;
    }

    public String getSavePointName() {
        return savePointName;
    }

    public void setSavePointName(String savePointName) {
        this.savePointName = savePointName;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }


    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public List<Param> getParamList() {
        return paramList;
    }

    public void setParamList(List<Param> paramList) {
        this.paramList = paramList;
    }

    public String getTranscationId() {
        return transcationId;
    }

    public void setTranscationId(String transcationId) {
        this.transcationId = transcationId;
    }

    public Boolean getAutoCommit() {
        return autoCommit;
    }

    public void setAutoCommit(Boolean autoCommit) {
        this.autoCommit = autoCommit;
    }

    public Integer getTransactionIsolation() {
        return transactionIsolation;
    }

    public void setTransactionIsolation(Integer transactionIsolation) {
        this.transactionIsolation = transactionIsolation;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }
}
