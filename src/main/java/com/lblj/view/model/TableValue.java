package com.lblj.view.model;

/**
 * @Maintainer Daniel.Ma
 * @Email danielma@zhuanxinbaoxian.com
 * @CreateDate 2023/9/14
 * @Version 1.0
 * @Comment
 */
public class TableValue {
    private String keyValue;
    private String valueValue;


    public TableValue(String keyValue, String valueValue) {
        this.keyValue = keyValue;
        this.valueValue = valueValue;
    }

    public TableValue() {
    }

    public String getKeyValue() {
        return keyValue;
    }

    public void setKeyValue(String keyValue) {
        this.keyValue = keyValue;
    }

    public String getValueValue() {
        return valueValue;
    }

    public void setValueValue(String valueValue) {
        this.valueValue = valueValue;
    }
}
