package com.izkml.database.exception;

import com.izkml.database.result.JsonResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.validation.BindException;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.ConstraintViolationException;
import java.util.stream.Collectors;

/**
 * 全局异常捕捉
 */
@ControllerAdvice
public class GlobalExceptionAdvisor {

    Logger logger = LoggerFactory.getLogger(GlobalExceptionAdvisor.class);

    private String getMessage(BindingResult result) {
        return result.getFieldErrors().stream().map(error -> {
            return error.getField() + "[" + error.getRejectedValue() + "]:" + error.getDefaultMessage();
        }).collect(Collectors.joining(", "));
    }

    /**
     * 捕获 controller 实体类参数@Validate验证报错
     *
     * @param e
     * @param request
     * @param response
     * @return
     */
    @ExceptionHandler(BindException.class)
    @ResponseBody
    public JsonResult validExceptionHandler(BindException e, HttpServletRequest request, HttpServletResponse response) {
        return JsonResult.error(getMessage(e.getBindingResult()));
    }

    /**
     * 捕获 普通方法传参@Validate验证报错
     *
     * @param e
     * @param request
     * @param response
     * @return
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    public JsonResult validExceptionHandler(MethodArgumentNotValidException e, HttpServletRequest request, HttpServletResponse response) {
        return JsonResult.error(getMessage(e.getBindingResult()));
    }

    /**
     * 捕获 controller 平铺参数@Validate验证报错
     *
     * @param e
     * @param request
     * @param response
     * @return
     */
    @ExceptionHandler(ConstraintViolationException.class)
    @ResponseBody
    public JsonResult validExceptionHandler(ConstraintViolationException e, HttpServletRequest request, HttpServletResponse response) {
        return JsonResult.error(e.getMessage());
    }


    @ExceptionHandler(Exception.class)
    @ResponseBody
    public JsonResult baseExceptionHandler(Exception e) {
        logger.error(e.getMessage(),e);
        return JsonResult.error(e.getMessage());
    }

}
