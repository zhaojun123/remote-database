package com.izkml.database.service;

import com.izkml.database.param.ParamWrapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

@Component
public class ConnectionPool {

    Logger logger = LoggerFactory.getLogger(ConnectionPool.class);

    @Autowired
    DataSource dataSource;

    private volatile boolean stop = false;

    /**
     * 心跳超过60秒 则认为链接失效
     */
    private final long pingTimeOut = 60000;

    private ExecutorService executorService = Executors.newSingleThreadExecutor();

    /**
     * 剔除已过期的connection
     */
    @PostConstruct
    private void init(){
        executorService.submit(()->{check();});
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            stopCheck();
        }));
    }

    private void stopCheck(){
        stop = true;
        executorService.shutdownNow();
    }

    private void check(){
        while(!stop){
            if(logger.isDebugEnabled()){
                logger.debug("check timeOut longer 60 s connection ");
            }
            Iterator<Map.Entry<String,ConnectionWrapper>> iterator = connectionMap.entrySet().iterator();
            while(iterator.hasNext()){
                Map.Entry<String,ConnectionWrapper> entry = iterator.next();
                if(System.currentTimeMillis() - entry.getValue().getLastTime()>pingTimeOut){
                    iterator.remove();
                    try {
                        entry.getValue().getConnection().close();
                        if(logger.isDebugEnabled()){
                            logger.debug("transcationId {} connection closed",entry.getKey());
                        }
                    } catch (SQLException e) {
                        logger.warn("transcationId {} connection close fail",entry.getKey(),e);
                    }
                }
            }
            //10秒检测
            LockSupport.parkNanos(1000*1000*1000*10L);
        }
    }

    /**
     * 未关闭的连接都暂存在这里
     */
    private static Map<String, ConnectionWrapper> connectionMap = new ConcurrentHashMap<>();

    public  Connection connect(ParamWrapper paramWrapper) throws SQLException {
        if(logger.isDebugEnabled()){
            logger.debug("init connection {}",paramWrapper);
        }
        String transcationId = paramWrapper.getTranscationId();
        Connection connection = dataSource.getConnection();
        connection.setAutoCommit(paramWrapper.getAutoCommit());
        connection.setTransactionIsolation(paramWrapper.getTransactionIsolation());
        connection.setSchema(paramWrapper.getSchema());
        synchronized (connectionMap) {
            //如果发现以有连接，则关闭老连接
            if(connectionMap.containsKey(transcationId)){
                ConnectionWrapper oldConnection = connectionMap.get(transcationId);
                try{
                    oldConnection.getConnection().close();
                    if(logger.isDebugEnabled()){
                        logger.debug("transcationId {} connection closed",transcationId);
                    }
                }catch (SQLException e){
                    logger.warn("transcationId {} connection close fail",transcationId,e);
                }
            }
            connectionMap.put(transcationId,new ConnectionWrapper(connection));
        }
        return connection;
    }

    public  Connection get(String transcationId) throws SQLException {
        ConnectionWrapper wrapper = connectionMap.get(transcationId);
        if(wrapper == null){
            throw new SQLException("not found connection transcationId "+transcationId);
        }
        wrapper.refresh();
        return wrapper.getConnection();
    }

    public void close(String transcationId){
        ConnectionWrapper wrapper = connectionMap.get(transcationId);
        if(wrapper!=null){
            try {
                wrapper.getConnection().close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
            connectionMap.remove(transcationId);
            if(logger.isDebugEnabled()){
                logger.debug("transcationId {} connection closed",transcationId);
            }
        }
    }

    public void ping(List<String> transcationIds){
        if(logger.isDebugEnabled()){
            logger.debug("receive connection ping {}",transcationIds);
        }
        connectionMap.entrySet().stream().forEach(entrySet->{
                if(transcationIds.contains(entrySet.getKey())){
                    entrySet.getValue().refresh();
                }
            }
        );
    }

}
