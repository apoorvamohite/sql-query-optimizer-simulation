package com.qosim;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */

/**
 *
 * @author apoor
 */
import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DbIndex {
    private String indexName;
    private Map<Integer, Character> indexColumnInfo;
    private Integer numIndexKeys;
    private Integer numIndexColumns;
    private List<DbIndexEntry> indexData;
    
    protected DbIndex(String fileName){
        readIndexFromFile(fileName);
    }
    
    protected String getIndexName(){
        return indexName;
    }
    
    protected Integer getNumIndexColumns(){
        return numIndexColumns;
    }
    
    protected Character getIndexColumnOrder(Integer i){
        return indexColumnInfo.get(i);
    }
    
    private void readIndexFromFile(String fileName){
        indexName = fileName;
        indexColumnInfo = new HashMap<Integer, Character>();
        indexData = new ArrayList<DbIndexEntry>();
        
        List<String> lines = DbUtil.readFileLines(DbConstants.DB_INDEXES_DIR_PATH + fileName + ".idx");
        
        // Read Columns and Datatypes
        String[] dataTypes = lines.get(0).split(",");
        for(String colType: dataTypes){
            if(colType.charAt(colType.length() -1) == DbConstants.ASC_COL){
                indexColumnInfo.put(Integer.parseInt(colType.substring(0, colType.length() -1)), DbConstants.ASC_COL);
            } else if(colType.charAt(colType.length() -1) == DbConstants.DESC_COL){
                indexColumnInfo.put(Integer.parseInt(colType.substring(0, colType.length() -1)), DbConstants.DESC_COL);
            }
        }
        
        numIndexColumns = indexColumnInfo.size();
        numIndexKeys = Integer.parseInt(lines.get(1));
        
        // Read Index Data
        Integer curColumn = 0;
        for(Integer i = 0; i<numIndexKeys; i++){
            String[] dataRow = lines.get(i+2).split(",");
            curColumn = 0;
            indexData.add(new DbIndexEntry(Integer.parseInt(dataRow[0]), dataRow[1]));
        }
    }
    
    protected Integer getRowLocation(){
        return 0;
    }
    
    protected static void getAllIndexes(String tableName) {
        File folder = new File(DbConstants.DB_INDEXES_DIR_PATH);
        File[] listOfFiles = folder.listFiles(new FilenameFilter(){
            @Override
            public boolean accept(File dir, String name){
                return name.endsWith(".idx") && name.startsWith(tableName);
            }
        });
        
        List<List<String>> indexTable = new ArrayList<List<String>>();
        indexTable.add(new ArrayList<>(List.of("Index Name")));
        
        DbTable tab = new DbTable(tableName);
        for(Integer i=1; i<=tab.getNumColumns(); i++){
            indexTable.get(0).add(i.toString()+(i==1?"st":i==2?"nd":i==3?"rd":"th") + " Column");
        }
        int j=1;
        for (File file: listOfFiles) {
            DbIndex idx = new DbIndex(file.getName().replace(".idx", ""));
            indexTable.add(new ArrayList<>(List.of(idx.getIndexName())));
            for(Integer i=1; i<=tab.getNumColumns(); i++){
                indexTable.get(j).add(idx.getIndexColumnOrder(i)!=null? (i.toString()+idx.getIndexColumnOrder(i)):"-");
            }
            j++;
        }
        DbUtil.formatTable(indexTable);
    }
    
    protected static void printAllIndexNames() {
        File folder = new File(DbConstants.DB_INDEXES_DIR_PATH);
        File[] listOfFiles = folder.listFiles(new FilenameFilter(){
            @Override
            public boolean accept(File dir, String name){
                return name.endsWith(".idx");
            }
        });
        for (int i = 0; i < listOfFiles.length; i++) {
            System.out.println("File " + listOfFiles[i].getName().replace(".idx", ""));
        }
    }
    
    protected class DbIndexEntry{
        private Integer RID;
        private String key;
        
        DbIndexEntry(Integer rid, String key){
            this.RID = rid;
            this.key = key;
        }
    }
}
