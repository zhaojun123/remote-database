package com.izkml.database.jdbc.json;

public class JsonFactory {

    private static Json json;

    private JsonFactory(){

    }

    static {
        initJson(()->{useJackson();});
        initJson(()->{useGson();});
        initJson(()->{usefastJson();});
    }

    private static void initJson(Runnable runnable){
        if(json == null){
            try{
                runnable.run();
            }catch (Exception e){

            }
        }
    }

    public static Json createJson(){
        return json;
    }

    private static void useJackson(){
        json = new JacksonImpl();
    }

    private static  void useGson(){
        json = new GsonImpl();
    }

    private static  void usefastJson(){
        json = new FastJsonImpl();
    }
}
