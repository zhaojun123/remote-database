package com.izkml.database.jdbc;

import com.izkml.database.jdbc.param.ParamWrapper;
import com.izkml.database.jdbc.param.StatementType;
import com.izkml.database.jdbc.parse.SqlParse;
import com.izkml.database.jdbc.parse.SqlType;
import com.izkml.database.jdbc.result.ResultWrapper;
import com.izkml.database.jdbc.utils.UnWrapUtils;
import com.izkml.database.jdbc.web.WebUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.util.ArrayList;
import java.util.List;

public class StatementImpl implements java.sql.Statement{

    private ConnectionImpl connection;

    private ResultSet resultSet;

    private int updateCount = -1;

    private volatile boolean close = false;


    public void checkClosed() throws SQLException {
        if(close){
            throw new SQLException("Statement.Operation not allowed after Statement closed");
        }
    }

    private List<ParamWrapper> batchList = new ArrayList<>();

    /**
     * 互斥锁
     */
    private Object mutex = new Object();

    public StatementImpl(ConnectionImpl connection){
        this.connection = connection;
    }

    public void setResultSet(ResultSet resultSet){
        this.resultSet = resultSet;
    }

    public void setUpdateCount(int updateCount){
        this.updateCount = updateCount;
    }

    public Object getMutex(){
        return mutex;
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        checkClosed();
        ParamWrapper paramWrapper = new ParamWrapper(connection,sql, StatementType.STATEMENT);
        ResultWrapper resultWrapper = WebUtils.doPostTemplate(connection.getWebUrl()+"/"+Constant.QUERY,paramWrapper, ResultWrapper.class);
        ResultSetImpl resultSet =  new ResultSetImpl(connection,this,resultWrapper);
        this.resultSet = resultSet;
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        checkClosed();
        this.resultSet = null;
        ParamWrapper paramWrapper = new ParamWrapper(connection,sql, StatementType.STATEMENT);
        int updateCount = WebUtils.doPostTemplate(connection.getWebUrl()+"/"+Constant.UPDATE,paramWrapper, Integer.class);
        setUpdateCount(updateCount);
        return updateCount;
    }

    @Override
    public void close() throws SQLException {
        if(connection == null){
            return;
        }
        synchronized (mutex){
            resultSet = null;
            connection = null;
            updateCount = -1;
        }
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
    }

    @Override
    public int getMaxRows() throws SQLException {
        return 0;
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return 0;
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {

    }

    @Override
    public void cancel() throws SQLException {

    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return null;
    }

    @Override
    public void clearWarnings() throws SQLException {

    }

    @Override
    public void setCursorName(String name) throws SQLException {

    }

    @Override
    public boolean execute(String sql) throws SQLException {
        checkClosed();
        SqlParse parse = new SqlParse(sql);
        parse.parse();
        if(parse.getSqlType()== SqlType.SELECT){
            executeQuery(sql);
        }else{
            executeUpdate(sql);
        }
        return true;
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        checkClosed();
        return resultSet;
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return resultSet!=null?-1:updateCount;
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return false;
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {

    }

    @Override
    public int getFetchDirection() throws SQLException {
        return ResultSet.FETCH_FORWARD;
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {

    }

    @Override
    public int getFetchSize() throws SQLException {
        return 1;
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return ResultSet.CONCUR_READ_ONLY;
    }

    @Override
    public int getResultSetType() throws SQLException {
        return ResultSet.TYPE_FORWARD_ONLY;
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        checkClosed();
        batchList.add(new ParamWrapper(connection,sql, StatementType.STATEMENT));
    }

    @Override
    public void clearBatch() throws SQLException {
        checkClosed();
        batchList.clear();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        checkClosed();
        int[] ints = WebUtils.doPostTemplate(connection.getWebUrl()+"/"+Constant.EXECUTE_BATCH,batchList, int[].class);
        clearBatch();
        return ints;
    }

    @Override
    public ConnectionImpl getConnection() throws SQLException {
        checkClosed();
        return connection;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return false;
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        checkClosed();
        ParamWrapper paramWrapper = new ParamWrapper(connection);
        ResultWrapper resultWrapper = WebUtils.doPostTemplate(connection.getWebUrl()+"/"+Constant.GENERATE_KEYS,paramWrapper, ResultWrapper.class);
        ResultSetImpl resultSet =  new ResultSetImpl(connection,this,resultWrapper);
        this.resultSet = resultSet;
        return resultSet;
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        return executeUpdate(sql);
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        return execute(sql);
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        return execute(sql);
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return java.sql.ResultSet.HOLD_CURSORS_OVER_COMMIT;
    }

    @Override
    public boolean isClosed() throws SQLException {
        return close;
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {

    }

    @Override
    public boolean isPoolable() throws SQLException {
        return false;
    }

    @Override
    public void closeOnCompletion() throws SQLException {

    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return false;
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
