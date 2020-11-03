package com.izkml.database.result;

public class JsonResult<T> {

    private boolean success = true;
    private T data;
    private String msg;
    private String errCode = "500";

    public boolean getSuccess() {
        return success;
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }

    public String getMsg() {
        return msg;
    }

    public void setMsg(String msg) {
        this.msg = msg;
    }

    public String getErrCode() {
        return errCode;
    }

    public void setErrCode(String errCode) {
        this.errCode = errCode;
    }

    public JsonResult(){}

    public JsonResult(boolean success, String errCode, String msg, T data) {
        this.success =success;
        this.errCode = errCode;
        this.msg = msg;
        this.data = data;
    }

    public static JsonResult ok() {
        return new JsonResult();
    }

    public static <T> JsonResult<T> ok(T data){
        return new JsonResult(true,null,null,data);
    }

    public static <T> JsonResult<T> ok(String msg,T data) {
        return new JsonResult(true, null,msg,data);
    }

    public static JsonResult error(String msg) {
        return new JsonResult(false, "500",msg,null);
    }

    public static JsonResult error(String errCode,String msg){
        return new JsonResult(false, errCode,msg,null);
    }

}
