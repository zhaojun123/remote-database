package com.izkml.database.jdbc;

import com.izkml.database.jdbc.param.ParamWrapper;
import com.izkml.database.jdbc.param.StatementType;
import com.izkml.database.jdbc.utils.SQLExceptionUtils;
import com.izkml.database.jdbc.utils.StringUtils;
import com.izkml.database.jdbc.utils.UnWrapUtils;
import com.izkml.database.jdbc.web.WebUtils;

import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;

public class ConnectionImpl implements Connection{

    private volatile boolean close = false;

    public void checkClosed() throws SQLException {
        if(close){
            throw new SQLException("Connection.Operation not allowed after Connection closed");
        }
    }

    /**
     * 事务标示
     */
    private String transcationId;

    /**
     * 互斥锁
     */
    private Object mutex;

    /**
     * jdbc:mysql:http://localhost:3306/database
     * jdbc:mysql:https://localhost:3306/database
     */
    private String url;

    /**
     * 用户
     */
    private String userName;

    /**
     * 密码
     */
    private String password;

    /**
     * web地址
     */
    private String webUrl;

    /**
     * 端口
     */
    private Integer port;

    private String protocol;

    private String host;

    private String schema;

    private Properties clientInfo;

    private boolean autoCommit = true;

    private int transactionIsolation = java.sql.Connection.TRANSACTION_READ_COMMITTED;

    public ConnectionImpl(String url,String userName,String password,Properties properties) throws SQLException {
        this.url = url;
        if(properties == null){
            properties = new Properties();
        }
        this.clientInfo = properties;
        this.userName = userName;
        this.password = password;
        this.transcationId = StringUtils.getUniqueId();
    }

    /**
     * 数据库产品名称 mysql、oracle等
     */
    private String databaseProductName;

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public String getProtocol() {
        return protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public Integer getPort() {
        return port;
    }

    public void setPort(Integer port) {
        this.port = port;
    }

    public String getDatabaseProductName() {
        return databaseProductName;
    }

    public void setDatabaseProductName(String databaseProductName) {
        this.databaseProductName = databaseProductName;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }

    public String getWebUrl() {
        return webUrl;
    }

    public void setWebUrl(String webUrl) {
        this.webUrl = webUrl;
    }

    @Override
    public Statement createStatement() throws SQLException {
        checkClosed();
        return new StatementImpl(this);
    }

    @Override
    public PreparedStatement prepareStatement(String sql) throws SQLException {
        return new PreparedStatementImpl(sql,this);
    }

    @Override
    public CallableStatement prepareCall(String sql) throws SQLException {
        throw SQLExceptionUtils.METHOD_NOT_SUPPORT();
    }

    @Override
    public String nativeSQL(String sql) throws SQLException {
        return sql;
    }

    @Override
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
    }

    @Override
    public boolean getAutoCommit() throws SQLException {
        return this.autoCommit;
    }

    @Override
    public void commit() throws SQLException {
        checkClosed();
        WebUtils.doPostTemplate(getWebUrl()+"/"+Constant.COMMIT,new ParamWrapper(this),null);
    }

    @Override
    public void rollback() throws SQLException {
        rollback(null);
    }

    @Override
    public void close() throws SQLException {
        RemotePing.remove(this);
        this.close = true;
        WebUtils.doPostTemplate(getWebUrl()+"/"+Constant.CLOSE,new ParamWrapper(this),null);
    }

    @Override
    public boolean isClosed() throws SQLException {
        return close;
    }

    @Override
    public DatabaseMetaData getMetaData() throws SQLException {
        return new DatabaseMetaDataImpl(this);
    }

    @Override
    public void setReadOnly(boolean readOnly) throws SQLException {

    }

    @Override
    public boolean isReadOnly() throws SQLException {
        return false;
    }

    @Override
    public void setCatalog(String catalog) throws SQLException {

    }

    @Override
    public String getCatalog() throws SQLException {
        return null;
    }

    @Override
    public void setTransactionIsolation(int level) throws SQLException {
        this.transactionIsolation = level;
    }

    @Override
    public int getTransactionIsolation() throws SQLException {
        return this.transactionIsolation;
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public Map<String, Class<?>> getTypeMap() throws SQLException {
        return null;
    }

    @Override
    public void setTypeMap(Map<String, Class<?>> map) throws SQLException {

    }

    @Override
    public void setHoldability(int holdability) throws SQLException {

    }

    @Override
    public int getHoldability() throws SQLException {
        return 0;
    }

    @Override
    public Savepoint setSavepoint() throws SQLException {
       return setSavepoint(null);
    }

    @Override
    public Savepoint setSavepoint(String name) throws SQLException {
        checkClosed();
        SavepointImpl savepoint = new SavepointImpl(name);
        ParamWrapper paramWrapper = new ParamWrapper(this);
        paramWrapper.setSavePointName(savepoint.getSavepointName());
        WebUtils.doPostTemplate(getWebUrl()+"/"+Constant.SAVEPOINT,paramWrapper,null);
        return savepoint;
    }

    @Override
    public void rollback(Savepoint savepoint) throws SQLException {
        checkClosed();
        ParamWrapper paramWrapper = new ParamWrapper(this);
        paramWrapper.setSavePointName(savepoint.getSavepointName());
        WebUtils.doPostTemplate(getWebUrl()+"/"+Constant.ROLL_BACK,paramWrapper,null);
    }

    @Override
    public void releaseSavepoint(Savepoint savepoint) throws SQLException {

    }

    @Override
    public Statement createStatement(int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return createStatement();
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public CallableStatement prepareCall(String sql, int resultSetType, int resultSetConcurrency, int resultSetHoldability) throws SQLException {
        return prepareCall(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLException {
        return prepareStatement(sql);
    }

    @Override
    public Clob createClob() throws SQLException {
        return null;
    }

    @Override
    public Blob createBlob() throws SQLException {
        return null;
    }

    @Override
    public NClob createNClob() throws SQLException {
        return null;
    }

    @Override
    public SQLXML createSQLXML() throws SQLException {
        return null;
    }

    @Override
    public boolean isValid(int timeout) throws SQLException {
        return true;
    }

    @Override
    public void setClientInfo(String name, String value) throws SQLClientInfoException {
        clientInfo.setProperty(name,value);
    }

    @Override
    public void setClientInfo(Properties properties) throws SQLClientInfoException {
        this.clientInfo = properties;
    }

    @Override
    public String getClientInfo(String name) throws SQLException {
        return clientInfo.getProperty(name);
    }

    @Override
    public Properties getClientInfo() throws SQLException {
        return clientInfo;
    }

    @Override
    public Array createArrayOf(String typeName, Object[] elements) throws SQLException {
        return null;
    }

    @Override
    public Struct createStruct(String typeName, Object[] attributes) throws SQLException {
        return null;
    }

    @Override
    public void setSchema(String schema) throws SQLException {
        this.schema = schema;
    }

    @Override
    public String getSchema() throws SQLException {
        return schema;
    }

    @Override
    public void abort(Executor executor) throws SQLException {

    }

    @Override
    public void setNetworkTimeout(Executor executor, int milliseconds) throws SQLException {

    }

    @Override
    public int getNetworkTimeout() throws SQLException {
        return 0;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return UnWrapUtils.unwrap(iface,this);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return UnWrapUtils.isWrapperFor(iface,this);
    }

    /**
     * 获取事务id
     * @return
     */
    public String getTranscationId() {
        return transcationId;
    }

}

