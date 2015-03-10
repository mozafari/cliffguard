package com.relationalcloud.tsqlparser.statement.create.table;


public class ColDataType {

    private String dataTypeName;
    //private List argumentsStringList;

/*    public List getArgumentsStringList() {
        return argumentsStringList;
    }*/

    public String getDataTypeName() {
        return dataTypeName;
    }

    /*public void setArgumentsStringList(List list) {
        argumentsStringList = list;
    }*/

    public void setDataTypeName(String string) {
        dataTypeName = string;
    }

    public String toString() {
        return dataTypeName/* + (argumentsStringList!=null?" "+PlainSelect.getStringList(argumentsStringList, true, true):"")*/;
    }
}