package com.qosim;


import java.util.List;
import java.util.Map;
import java.util.ArrayList;
import java.util.HashMap;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author apoor
 */
public class DbTable {
    private String tableName;
    private Integer numColumns;
    private List<Character> columnType;
    private List<Integer> columnLength;
    private List<Integer> columnCardinality;
    private Integer numRows;
    private Map<Integer, List> data;
    private Map<Integer, ColumnDefinition> columnDefn;
    
    
    protected DbTable(String fileName){
        readTableFromFile(fileName);
    }
    
    protected Integer getNumColumns(){
        return numColumns;
    }
    
    protected Integer getNumRows(){
        return numRows;
    }
    
    protected Character getColumnType(Integer i){
        return columnType.get(i);
    }
    
    protected ColumnDefinition getColumnDefn(Integer i){
        return columnDefn.get(i);
    }
    
    protected Map<Integer, List> getData(){
        return data;
    }
    
    private void readTableFromFile(String fileName){
        columnType = new ArrayList<Character>();
        columnLength = new ArrayList<Integer>();
        columnCardinality = new ArrayList<Integer>();
        data = new HashMap<Integer, List>();
        columnDefn = new HashMap<Integer, ColumnDefinition>();
        
        List<String> lines = DbUtil.readFileLines(DbConstants.DB_TABLES_DIR_PATH + fileName + ".tab");
        Integer columnNo = 1;
        
        // Read Datatypes
        String[] dataTypes = lines.get(0).split(",");
        for(String colType: dataTypes){
            columnType.add(colType.charAt(0));
            if(colType.charAt(0) == DbConstants.CHAR_DATA_TYPE){
                Integer length = Integer.parseInt(colType.substring(1));
                columnDefn.put(columnNo, new ColumnDefinition(DbConstants.CHAR_DATA_TYPE, length));
                columnLength.add(length);
                data.put(columnNo++, new ArrayList<String>());
            } else {
                columnDefn.put(columnNo, new ColumnDefinition(DbConstants.INT_DATA_TYPE, 0));
                columnLength.add(0);
                data.put(columnNo++, new ArrayList<Integer>());
            }
        }
        
        numColumns = columnType.size();
        
        // Read Cardinalities
        String[] cardinality = lines.get(1).split(",");
        for(String card: cardinality){
            columnCardinality.add(Integer.parseInt(card));
        }
        
        // Read numRows
        numRows = Integer.parseInt(lines.get(2));
        Integer curColumn = 0;
        for(Integer i = 0; i<numRows; i++){
            String[] dataRow = lines.get(i+3).split(",");
            curColumn = 0;
            for(String s: dataRow){
                if(columnType.get(curColumn) == DbConstants.CHAR_DATA_TYPE){
                    data.get(curColumn+1).add(s);
                } else if(columnType.get(curColumn) == DbConstants.INT_DATA_TYPE){
                    data.get(curColumn+1).add(Integer.parseInt(s));
                }
                curColumn++;
            }
        }
    }
    
    protected class ColumnDefinition{
        private Character dataType;
        private Integer dataLength;
        
        protected Integer getDataLength(){
            return this.dataLength;
        }
        
        protected Character getDataType(){
            return this.dataType;
        }
        
        protected ColumnDefinition(Character dataType, Integer length){
            this.dataLength = length;
            this.dataType = dataType;
        }
    }
}
