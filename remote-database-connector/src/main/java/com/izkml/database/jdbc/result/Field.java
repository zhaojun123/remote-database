package com.izkml.database.jdbc.result;

public class Field {

    private Boolean autoIncrement;

    /**
     * 大小写铭感
     */
    private Boolean caseSensitive;

    /**
     * 是否是货币数额
     */
    private Boolean currency;

    /**
     * columnNoNulls、columnNullable、columnNullableUnknown
     */
    private Integer nullable;

    /**
     * 有正负号的数字
     */
    private Boolean signed;

    /**
     * the normal maximum number of characters
     */
    private Integer columnDisplaySize;

    /**
     * 字段别名
     */
    private String columnLabel;

    private String columnName;

    /**
     * column's table's schema
     */
    private String schemaName;

    /**
     * 字符长度或者数字精度
     */
    private Integer precision;

    /**
     * 小数点后位数
     */
    private Integer scale;

    private String tableName;

    /**
     * 所属表的目录名称
     */
    private String catalogName;

    /**
     * 字段类型 {@link java.sql.Types}
     */
    private int columnType;

    /**
     * 字段类型名称
     */
    private String columnTypeName;

    private Boolean readOnly;

    private Boolean writable;

    /**
     * Indicates whether a write on the designated column will definitely succeed
     */
    private Boolean definitelyWritable;

    /**
     * 字段的类型 对应的 java class
     */
    private String columnClassName;


    public Boolean getAutoIncrement() {
        return autoIncrement;
    }

    public void setAutoIncrement(Boolean autoIncrement) {
        this.autoIncrement = autoIncrement;
    }

    public Boolean getCaseSensitive() {
        return caseSensitive;
    }

    public void setCaseSensitive(Boolean caseSensitive) {
        this.caseSensitive = caseSensitive;
    }

    public Boolean getCurrency() {
        return currency;
    }

    public void setCurrency(Boolean currency) {
        this.currency = currency;
    }

    public Integer getNullable() {
        return nullable;
    }

    public void setNullable(Integer nullable) {
        this.nullable = nullable;
    }

    public Boolean getSigned() {
        return signed;
    }

    public void setSigned(Boolean signed) {
        this.signed = signed;
    }

    public Integer getColumnDisplaySize() {
        return columnDisplaySize;
    }

    public void setColumnDisplaySize(Integer columnDisplaySize) {
        this.columnDisplaySize = columnDisplaySize;
    }

    public String getColumnLabel() {
        return columnLabel;
    }

    public void setColumnLabel(String columnLabel) {
        this.columnLabel = columnLabel;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public Integer getPrecision() {
        return precision;
    }

    public void setPrecision(Integer precision) {
        this.precision = precision;
    }

    public Integer getScale() {
        return scale;
    }

    public void setScale(Integer scale) {
        this.scale = scale;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public int getColumnType() {
        return columnType;
    }

    public void setColumnType(int columnType) {
        this.columnType = columnType;
    }

    public String getColumnTypeName() {
        return columnTypeName;
    }

    public void setColumnTypeName(String columnTypeName) {
        this.columnTypeName = columnTypeName;
    }

    public Boolean getReadOnly() {
        return readOnly;
    }

    public void setReadOnly(Boolean readOnly) {
        this.readOnly = readOnly;
    }

    public Boolean getWritable() {
        return writable;
    }

    public void setWritable(Boolean writable) {
        this.writable = writable;
    }

    public Boolean getDefinitelyWritable() {
        return definitelyWritable;
    }

    public void setDefinitelyWritable(Boolean definitelyWritable) {
        this.definitelyWritable = definitelyWritable;
    }

    public String getColumnClassName() {
        return columnClassName;
    }

    public void setColumnClassName(String columnClassName) {
        this.columnClassName = columnClassName;
    }
}
