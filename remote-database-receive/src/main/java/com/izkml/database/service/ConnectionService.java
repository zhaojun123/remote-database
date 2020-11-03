package com.izkml.database.service;

import com.izkml.database.param.ParamWrapper;
import com.izkml.database.param.StatementType;
import com.izkml.database.parse.PreparedStatementParse;
import com.izkml.database.parse.ResultSetParse;
import com.izkml.database.result.Field;
import com.izkml.database.result.ResultWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.sql.DataSource;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class ConnectionService {

    Logger logger = LoggerFactory.getLogger(ConnectionService.class);

    @Autowired
    ConnectionPool connectionPool;

    public Connection getConnection(ParamWrapper paramWrapper) throws SQLException {
        if(logger.isDebugEnabled()){
            logger.debug("get connection transcationId {}",paramWrapper.getTranscationId());
        }
        String transcationId = paramWrapper.getTranscationId();
        Connection connection = connectionPool.get(transcationId);
        return connection;
    }

    /**
     * 数据库链接
     * @param paramWrapper
     * @return
     * @throws SQLException
     */
    public Connection connect(ParamWrapper paramWrapper) throws SQLException {
        if(logger.isDebugEnabled()){
            logger.debug("connect transcationId :{}",paramWrapper.getTranscationId());
        }
        return connectionPool.connect(paramWrapper);
    }

    /**
     * 查询
     * @param paramWrapper
     * @param statementType
     * @return
     * @throws SQLException
     */
    public ResultWrapper query(ParamWrapper paramWrapper,StatementType statementType) throws SQLException{
        if(logger.isDebugEnabled()){
            logger.debug("query transcationId :{} sql :{} params :{}",paramWrapper.getTranscationId(),paramWrapper.getSql(),paramWrapper.getParamList());
        }
        if(statementType == null){
            statementType = StatementType.PREPAREDSTATEMENT;
        }
        Connection connection = null;
        Statement statement = null;
        try{
            connection = getConnection(paramWrapper);
            statement = getStatement(connection,paramWrapper,statementType);
            ResultSet resultSet = null;
            if(statementType == StatementType.PREPAREDSTATEMENT){
                resultSet = ((PreparedStatement)statement).executeQuery();
            }else{
                resultSet = statement.executeQuery(paramWrapper.getSql());
            }
            return getResult(resultSet);
        }finally {
            closeStatement(statement);
        }
    }

    /**
     * update操作
     * @param paramWrapper
     * @param statementType
     * @return
     * @throws SQLException
     */
    public int update(ParamWrapper paramWrapper,StatementType statementType) throws SQLException {
        if(logger.isDebugEnabled()){
            logger.debug("update transcationId :{} sql :{} params :{}",paramWrapper.getTranscationId(),paramWrapper.getSql(),paramWrapper.getParamList());
        }
        if(statementType == null){
            statementType = StatementType.PREPAREDSTATEMENT;
        }
        Connection connection = null;
        Statement statement = null;
        try{
            connection = getConnection(paramWrapper);
            statement = getStatement(connection,paramWrapper,statementType);
            if(statementType == StatementType.PREPAREDSTATEMENT){
                ((PreparedStatement)statement).execute();
            }else{
                statement.execute(paramWrapper.getSql());
            }
            return statement.getUpdateCount();
        }finally {
            closeStatement(statement);
        }
    }

    /**
     * update 批量
     * @param paramWrapperList
     * @return
     * @throws SQLException
     */
    public int[] updateBatch(List<ParamWrapper> paramWrapperList) throws SQLException {
        if(logger.isDebugEnabled()){
            for(ParamWrapper paramWrapper:paramWrapperList){
                logger.debug("updateBatch transcationId :{} sql :{} params :{}",paramWrapper.getTranscationId(),paramWrapper.getSql(),paramWrapper.getParamList());
            }
        }
        Connection connection = null;
        Statement statement = null;
        try {
            connection = getConnection(paramWrapperList.get(0));
            statement = getStatement(connection,paramWrapperList);
            return statement.executeBatch();
        }finally {
            closeStatement(statement);
        }
    }

    /**
     * 事务提交
     * @param paramWrapper
     * @throws SQLException
     */
    public void commit(ParamWrapper paramWrapper) throws SQLException {
        if(logger.isDebugEnabled()){
            logger.debug("commit transcationId :{}",paramWrapper.getTranscationId());
        }
        Connection connection = getConnection(paramWrapper);
        connection.commit();
    }

    /**
     * 事务回滚
     * @param paramWrapper
     * @throws SQLException
     */
    public void rollBack(ParamWrapper paramWrapper)throws SQLException{
        if(logger.isDebugEnabled()){
            logger.debug("rollBack transcationId :{}",paramWrapper.getTranscationId());
        }
        Connection connection = getConnection(paramWrapper);
        if(paramWrapper.getSavePointName()!=null){
            connection.rollback(new Savepoint() {
                @Override
                public int getSavepointId() throws SQLException {
                    return 0;
                }

                @Override
                public String getSavepointName() throws SQLException {
                    return paramWrapper.getSavePointName();
                }
            });
        }else{
            connection.rollback();
        }
    }

    /**
     * 获取自增的id
     * @param paramWrapper
     * @return
     * @throws SQLException
     */
    public ResultWrapper generatedKeys(ParamWrapper paramWrapper) throws SQLException {
        if(logger.isDebugEnabled()){
            logger.debug("generatedKeys transcationId :{}",paramWrapper.getTranscationId());
        }
        Connection connection = getConnection(paramWrapper);
        Statement statement = null;
        try{
            statement = connection.createStatement();
            return getResult(statement.getGeneratedKeys());
        }finally {
            closeStatement(statement);
        }
    }

    /**
     * 设置安全点
     * @param paramWrapper
     * @throws SQLException
     */
    public void savepoint(ParamWrapper paramWrapper) throws SQLException {
        if(logger.isDebugEnabled()){
            logger.debug("savepoint transcationId :{}",paramWrapper.getTranscationId());
        }
        Connection connection = getConnection(paramWrapper);
        connection.setSavepoint(paramWrapper.getSavePointName());
    }

    /**
     * 关闭链接
     * @param paramWrapper
     * @throws SQLException
     */
    public void close(ParamWrapper paramWrapper) throws SQLException{
        if(logger.isDebugEnabled()){
            logger.debug("close transcationId :{}",paramWrapper.getTranscationId());
        }
        connectionPool.close(paramWrapper.getTranscationId());
    }

    /**
     * ping
     * @param transcationIds
     */
    public void ping(List<String> transcationIds){
        connectionPool.ping(transcationIds);
    }

    private void closeStatement(Statement statement){
        if(statement!=null){
            try {
                statement.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }


    private Statement getStatement(Connection connection,ParamWrapper paramWrapper,StatementType statementType) throws SQLException {
        Statement statement = null;
        if(StatementType.STATEMENT == statementType){
            statement = connection.createStatement();
        }else{
            statement = connection.prepareStatement(paramWrapper.getSql());
            PreparedStatementParse parse = new PreparedStatementParse((PreparedStatement) statement,paramWrapper);
            parse.parse();
        }
        return statement;
    }

    private Statement getStatement(Connection connection,List<ParamWrapper> paramList) throws SQLException {
        Statement statement = null;
        if(StatementType.STATEMENT == paramList.get(0).getStatementType()){
            statement = connection.createStatement();
            for(ParamWrapper paramWrapper:paramList){
                statement.addBatch(paramWrapper.getSql());
            }
        }else{
            statement = connection.prepareStatement(paramList.get(0).getSql());
            for(ParamWrapper paramWrapper:paramList){
                PreparedStatementParse parse = new PreparedStatementParse((PreparedStatement) statement,paramWrapper);
                parse.parse();
                ((PreparedStatement) statement).addBatch();
            }
        }
        return statement;
    }

    private ResultWrapper getResult(ResultSet resultSet) throws SQLException {
        try{
            ResultSetParse resultSetParse = new ResultSetParse(resultSet);
            return resultSetParse.parse();
        }finally {
            try {
                resultSet.close();
            }catch (SQLException e){
                e.printStackTrace();
            }
        }
    }

}
