package com.izkml.database.web;

import com.izkml.database.param.ParamWrapper;
import com.izkml.database.result.JsonResult;
import com.izkml.database.result.ResultWrapper;
import com.izkml.database.service.ConnectionService;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.sql.SQLException;
import java.util.List;

/**
 * sql 接收
 */
@RestController
public class SqlController {

    @Autowired
    ConnectionService connectionService;

    @RequestMapping("/query")
    public JsonResult<ResultWrapper> query(@Validated @RequestBody ParamWrapper paramWrapper) throws SQLException {
        ResultWrapper resultWrapper = connectionService.query(paramWrapper,paramWrapper.getStatementType());
        return JsonResult.ok(resultWrapper);
    }

    @RequestMapping("/update")
    public JsonResult<Integer> update(@Validated @RequestBody ParamWrapper paramWrapper) throws SQLException{
        int updateCount = connectionService.update(paramWrapper,paramWrapper.getStatementType());
        return JsonResult.ok(updateCount);
    }

    @RequestMapping("/executeBatch")
    public JsonResult<int[]> executeBatch(@Validated @RequestBody List<ParamWrapper> paramWrapperList)throws SQLException{
        int[] updateCount = connectionService.updateBatch(paramWrapperList);
        return JsonResult.ok(updateCount);
    }

    @RequestMapping("/commit")
    public JsonResult commit(@Validated @RequestBody ParamWrapper paramWrapper)throws SQLException{
        connectionService.commit(paramWrapper);
        return JsonResult.ok();
    }

    @RequestMapping("/rollBack")
    public JsonResult rollBack(@Validated @RequestBody ParamWrapper paramWrapper)throws SQLException{
        connectionService.rollBack(paramWrapper);
        return JsonResult.ok();
    }

    @RequestMapping("/generatedKeys")
    public JsonResult<ResultWrapper> generatedKeys(@Validated @RequestBody ParamWrapper paramWrapper)throws SQLException{
        return JsonResult.ok(connectionService.generatedKeys(paramWrapper));
    }

    @RequestMapping("/savepoint")
    public JsonResult savepoint(@Validated @RequestBody ParamWrapper paramWrapper)throws SQLException{
        connectionService.savepoint(paramWrapper);
        return JsonResult.ok();
    }

    @RequestMapping("/close")
    public JsonResult close(@Validated @RequestBody ParamWrapper paramWrapper)throws SQLException{
        connectionService.close(paramWrapper);
        return JsonResult.ok();
    }

    @RequestMapping("/connect")
    public JsonResult connect(@Validated @RequestBody ParamWrapper paramWrapper)throws SQLException{
        connectionService.connect(paramWrapper);
        return JsonResult.ok();
    }

    @RequestMapping("/ping")
    public JsonResult ping(@Validated @RequestBody List<String> transcationIds)throws SQLException{
        connectionService.ping(transcationIds);
        return JsonResult.ok();
    }
}
