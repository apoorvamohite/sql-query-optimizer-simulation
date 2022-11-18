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
import java.util.Collections;
import java.util.Comparator;
import org.apache.commons.lang3.StringUtils;

public class DbIndex {

    private String indexName;
    private Map<Integer, Character> indexColumnInfo;
    private Integer numIndexKeys;
    private Integer numIndexColumns;
    private List<DbIndexEntry> indexData;
    private List<Integer> indexColumns;
    private List<Character> indexColumnOrder;

    protected DbIndex(String fileName) {
        readIndexFromFile(fileName);
    }

    protected DbIndex(String tableName, List<String> columns) {
        generateIndexEntries(tableName, columns);
    }

    protected List<DbIndexEntry> getIndexData() {
        return this.indexData;
    }

    protected List<Integer> getIndexColumns() {
        return this.indexColumns;
    }

    protected List<Character> getIndexColumnOrder() {
        return this.indexColumnOrder;
    }

    protected String getIndexName() {
        return indexName;
    }

    protected Integer getNumIndexColumns() {
        return numIndexColumns;
    }

    protected Character getIndexColumnOrder(Integer i) {
        return indexColumnInfo.get(i);
    }

    protected void generateIndexEntries(String tableName, List<String> columns) {
        indexColumns = new ArrayList<Integer>();
        indexColumnOrder = new ArrayList<Character>();
        indexData = new ArrayList<DbIndexEntry>();
        DbTable table = new DbTable(tableName);
        Map<Integer, List> tableData = table.getData();
        Integer[] ordering = new Integer[columns.size()];

        for (Integer row = 0; row < table.getNumRows(); row++) {
            String key = "";
            List<String> indexKeyValues = new ArrayList<String>();
            List<DbTable.ColumnDefinition> colDefinitions = new ArrayList<DbTable.ColumnDefinition>();
            for (Integer j = 0; j < columns.size(); j++) {
                String colDefn = columns.get(j);
                Character order = colDefn.charAt(colDefn.length() - 1);
                Integer columnNum = Integer.parseInt(colDefn.substring(0, colDefn.length() - 1));

                indexKeyValues.add(tableData.get(columnNum).get(row).toString());
                colDefinitions.add(table.getColumnDefn(columnNum));

                // TODO: take this out
                if (row == 0) {
                    indexColumns.add(columnNum);
                    indexColumnOrder.add(order);
                    ordering[j] = (order == DbConstants.ASC_COL ? 1 : -1);
                }
            }
            indexData.add(new DbIndexEntry(row, key, indexKeyValues, colDefinitions, indexColumnOrder));
        }
        // Sort index key
        Collections.sort(indexData, new Comparator<DbIndexEntry>() {
            @Override
            public int compare(final DbIndexEntry lhs, DbIndexEntry rhs) {
                if (lhs.getKey().compareTo(rhs.getKey()) > 0) {
                    return 1;
                } else if (lhs.getKey().compareTo(rhs.getKey()) < 0) {
                    return -1;
                } else {
                    return 0;
                }
            }
        });
    }

    private void readIndexFromFile(String fileName) {
        indexName = fileName;
        indexColumnInfo = new HashMap<Integer, Character>();
        indexData = new ArrayList<DbIndexEntry>();
        indexColumns = new ArrayList<Integer>();
        indexColumnOrder = new ArrayList<Character>();

        List<String> lines = DbUtil.readFileLines(DbConstants.DB_INDEXES_DIR_PATH + fileName + ".idx");

        // Read Columns and Datatypes
        String[] dataTypes = lines.get(0).split(",");
        for (String colType : dataTypes) {
            Integer colNum = Integer.parseInt(colType.substring(0, colType.length() - 1));
            if (colType.charAt(colType.length() - 1) == DbConstants.ASC_COL) {
                indexColumns.add(colNum);
                indexColumnOrder.add(DbConstants.ASC_COL);
                indexColumnInfo.put(colNum, DbConstants.ASC_COL);
            } else if (colType.charAt(colType.length() - 1) == DbConstants.DESC_COL) {
                indexColumns.add(colNum);
                indexColumnOrder.add(DbConstants.DESC_COL);
                indexColumnInfo.put(colNum, DbConstants.DESC_COL);
            }
        }

        numIndexColumns = indexColumnInfo.size();
        numIndexKeys = Integer.parseInt(lines.get(1));

        // Read Index Data
        Integer curColumn = 0;
        for (Integer i = 0; i < numIndexKeys; i++) {
            String[] dataRow = lines.get(i + 2).split(" ", 2);
            curColumn = 0;
            indexData.add(new DbIndexEntry(Integer.parseInt(dataRow[0]), dataRow[1]));
        }
    }

    protected Integer getRowLocation() {
        return 0;
    }

    protected static void getAllIndexes(String tableName) {
        File folder = new File(DbConstants.DB_INDEXES_DIR_PATH);
        File[] listOfFiles = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".idx") && name.startsWith(tableName);
            }
        });

        List<List<String>> indexTable = new ArrayList<List<String>>();
        indexTable.add(new ArrayList<>(List.of("Index Name")));

        // TODO: No need to read data. Just column definition
        DbTable tab = new DbTable(tableName);
        for (Integer i = 1; i <= tab.getNumColumns(); i++) {
            indexTable.get(0).add(i.toString() + (i == 1 ? "st" : i == 2 ? "nd" : i == 3 ? "rd" : "th") + " Column");
        }
        int j = 1;
        for (File file : listOfFiles) {
            DbIndex idx = new DbIndex(file.getName().replace(".idx", ""));
            indexTable.add(new ArrayList<>(List.of(idx.getIndexName())));
            for (Integer i = 1; i <= tab.getNumColumns(); i++) {
                indexTable.get(j).add(idx.getIndexColumnOrder(i) != null ? (i.toString() + idx.getIndexColumnOrder(i)) : "-");
            }
            j++;
        }
        DbUtil.formatTable(indexTable);
    }

    protected static void printAllIndexNames() {
        File folder = new File(DbConstants.DB_INDEXES_DIR_PATH);
        File[] listOfFiles = folder.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".idx");
            }
        });
        for (int i = 0; i < listOfFiles.length; i++) {
            System.out.println("File " + listOfFiles[i].getName().replace(".idx", ""));
        }
    }

    protected class DbIndexEntry {

        private Integer RID;
        private String key;
        private List<String> indexKeyValues;

        protected List<String> getIndexKeyValues() {
            return this.indexKeyValues;
        }

        protected Integer getRID() {
            return this.RID;
        }

        protected String getKey() {
            return this.key;
        }

        protected DbIndexEntry(Integer rid, String key, List<String> indexKeyValues) {
            this.RID = rid;
            this.key = key;
            this.indexKeyValues = indexKeyValues;
        }

        protected DbIndexEntry(Integer rid, String key, List<String> indexKeyValues, List<DbTable.ColumnDefinition> colDefn, List<Character> order) {
            this.RID = rid;
            this.key = this.generateKey(indexKeyValues, colDefn, order);
            this.indexKeyValues = indexKeyValues;
        }

        protected DbIndexEntry(Integer rid, String key) {
            this.RID = rid;
            this.key = key;
        }

        private String generateKey(List<String> indexKeyValues, List<DbTable.ColumnDefinition> colDefinitions, List<Character> order) {
            String res = "";
            Integer curCol = 0;
            StringBuilder sb = new StringBuilder();
            for (DbTable.ColumnDefinition colDefn : colDefinitions) {
                // TODO:handle ordering, use StringBuilder
                if (colDefn.getDataType() == DbConstants.CHAR_DATA_TYPE) {
                    if (order.get(curCol) == DbConstants.DESC_COL) {
                        sb.append(StringUtils.rightPad(DbUtil.complementString(indexKeyValues.get(curCol)), colDefn.getDataLength()));
                    } else {
                        sb.append(StringUtils.rightPad(indexKeyValues.get(curCol), colDefn.getDataLength()));
                    }
                } else {
                    if (order.get(curCol) == DbConstants.DESC_COL) {
                        sb.append(StringUtils.leftPad(DbUtil.complementInteger(indexKeyValues.get(curCol)), colDefn.getDataLength(), "0"));
                    } else {
                        sb.append(StringUtils.leftPad(indexKeyValues.get(curCol), colDefn.getDataLength(), "0"));
                    }
                }
                curCol++;
            }
            return sb.toString();
        }
    }

}
