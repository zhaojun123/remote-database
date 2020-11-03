package com.izkml.database.jdbc.result;

import java.util.ArrayList;
import java.util.List;

/**
 * 远程数据返回包装类
 */
public class ResultWrapper {

    /**
     * 字段的属性
     */
    private List<Field> fields;

    /**
     * 返回的数据
     */
    private List<List<Object>> results = new ArrayList();

    public List<Field> getFields() {
        return fields;
    }

    public void setFields(List<Field> fields) {
        this.fields = fields;
    }

    public List<List<Object>> getResults() {
        return results;
    }

    public void setResults(List<List<Object>> results) {
        this.results = results;
    }

}
