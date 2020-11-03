package com.izkml.database.jdbc;

import com.izkml.database.jdbc.ConnectionImpl;
import com.izkml.database.jdbc.web.WebUtils;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

class RemotePing {

    private static volatile boolean stop = false;

    private final static long heartbeatTime = 10000; //间隔十秒心跳检测一次

    private final static int connectTimeout = 5000;

    private final static int readTimeout = 5000;

    private static ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(1,10,60, TimeUnit.SECONDS,new ArrayBlockingQueue<>(100));

    /**
     * 未关闭的connection
     */
    private static ConcurrentHashMap<String, ConnectionImpl> aliveConnection = new ConcurrentHashMap();

    static {
        Thread pingThread = new Thread(()->{ping();});
        //守护线程
        pingThread.setDaemon(true);
        pingThread.start();
        Runtime.getRuntime().addShutdownHook(new Thread(()->{
            RemotePing.stopPing();
        }));
    }

    public static void regist(ConnectionImpl connection){
        aliveConnection.put(connection.getTranscationId(),connection);
    }

    public static void remove(ConnectionImpl connection){
        aliveConnection.remove(connection.getTranscationId());
    }

    public static void stopPing(){
        System.out.println("stop the remote database ping");
        stop = true;
        threadPoolExecutor.shutdownNow();
    }

    private static void ping(){
        while(!stop){
            if(!aliveConnection.isEmpty()){
                //因为可能存在多数据源，所以ping的地址可能有多个
                Map<String,List<String>> map = new HashMap<>();
                aliveConnection.values().forEach(entry->{
                    List<String> list = map.getOrDefault(entry.getWebUrl(),new ArrayList<>());
                    list.add(entry.getTranscationId());
                    map.put(entry.getWebUrl(),list);
                });
                map.entrySet().forEach(entry->{
                        threadPoolExecutor.execute(()->{
                            try {
                                WebUtils.doPostTemplate(entry.getKey()+"/"+Constant.PING,entry.getValue(),null,connectTimeout,readTimeout,null);
                            } catch (SQLException e) {
                                e.printStackTrace();
                            }
                        });
                });
            }
            LockSupport.parkNanos(heartbeatTime*1000*1000);
        }
    }

}
