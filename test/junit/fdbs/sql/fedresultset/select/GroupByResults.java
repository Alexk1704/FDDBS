/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package junit.fdbs.sql.fedresultset.select;

import java.util.HashMap;

/**
 * @author Markus
 */
public class GroupByResults{

    HashMap<Integer, Integer> intValues;
    HashMap<String, Integer> charValues;
    int leftColumnType;
    int rightColumnType;
    String leftAttributeName;
    String rightAttributeName;
    int amountRows;

    public GroupByResults(int leftColumnsType, int rightColumnsType, String leftAttributeName,
                          String rightAttributeName){
        amountRows = 0;
        this.intValues = new HashMap<>();
        this.charValues = new HashMap<>();
        this.leftColumnType = leftColumnsType;
        this.rightColumnType = rightColumnsType;
        this.leftAttributeName = leftAttributeName;
        this.rightAttributeName = rightAttributeName;
    }

    public void setStringRowResult(String key, int keyResult){
        this.charValues.put(key, keyResult);
    }

    public void setIntegerRowResult(int key, int keyResult){
        this.intValues.put(key, keyResult);
    }

    public int getCharRowResult(String key){
        return charValues.get(key);
    }

    public int getIntegerRowResult(int key){
        return intValues.get(key);
    }

    public int getStringRowResult(String key){
        return charValues.get(key);
    }

    public boolean containsStringRowKey(String keyName){
        return charValues.containsKey(keyName);
    }

    public boolean containsIntegerRowKey(int keyNr){
        return intValues.containsKey(keyNr);
    }

    public int getLeftType(){

        //https://docs.oracle.com/javase/7/docs/api/constant-values.html#java.sql.Types.VARCHAR
        return leftColumnType;
    }

    public int getRightType(){
        //https://docs.oracle.com/javase/7/docs/api/constant-values.html#java.sql.Types.VARCHAR
        return rightColumnType;
    }

    public String getLeftAttributeName(){
        return leftAttributeName;
    }

    public String getRightAttributeName(){
        return rightAttributeName;
    }

    public int getAmountRows(){
        return amountRows;
    }

    public void setAmountRows(int amountRows){
        this.amountRows = amountRows;
    }

    public void increaseAmountRows(){
        amountRows++;
    }
}
