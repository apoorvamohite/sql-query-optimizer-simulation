/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Project/Maven2/JavaApp/src/main/java/${packagePath}/${mainClassName}.java to edit this template
 */
package com.qosim;

import net.sf.jsqlparser.*;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.util.TablesNamesFinder;
import net.sf.jsqlparser.statement.drop.Drop;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import java.util.List;
import net.sf.jsqlparser.JSQLParserException;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;

/**
 *
 * @author apoor
 */
public class Qosim {

    public static void main(String[] args) {
        List<String> statements = DbUtil.readFileLines(args[0]);
        for (String sql : statements) {
            try {
                Statement stmt = (Statement) CCJSqlParserUtil.parse(sql);
                String className = stmt.getClass().getSimpleName();
                switch (className) {
                    case "CreateIndex":
                        CreateIndex ci = (CreateIndex) stmt;
                        createIndex(ci);
                        break;
                    case "Select":
                        System.out.println(className);
                        PlainSelect sel = (PlainSelect) ((Select) stmt).getSelectBody();
                        System.out.println(sel.getSelectItems().get(0));
                        break;
                    case "Drop":
                        System.out.println(className);
                        break;
                    default:
                        System.out.println("this is default");
                }


                /*System.out.println(stmt);
                TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
                List<String> tableNames = tablesNamesFinder.getTableList(stmt);
                for (String tableName : tableNames) {
                    System.out.println(tableName);
                }
                PlainSelect ps = (PlainSelect) stmt.getSelectBody();
                System.out.println(ps.getSelectItems());
                System.out.println(ps.getFromItem());
                System.out.println(ps.getJoins());
                System.out.println(ps.getWhere());*/
            } catch (JSQLParserException pex) {
                if (sql.startsWith("LIST INDEX")) {
                    String tableName = sql.split(" ")[2];
                    System.out.println("List index on " + tableName);
                    DbIndex.getAllIndexes(tableName);
                }
            } catch (Exception e) {
                throw e;
            }
        }

    }

    protected static void createIndex(CreateIndex ci) {
        String fileName = ci.getTable().getName()+ ci.getIndex().getName();
        File idxFile = new File(DbConstants.DB_INDEXES_DIR_PATH+fileName+DbConstants.DB_INDEX_FILE_EXT);
        DbIndex idx = new DbIndex(ci.getTable().getName(), ci.getIndex().getColumnsNames());
        try{
            if (idxFile.createNewFile()) {
                System.out.println("File created: " + idxFile.getName());
                FileWriter writer = new FileWriter(idxFile);
                List<Integer> indexColumns = idx.getIndexColumns();
                List<Character> indexColumnOrder = idx.getIndexColumnOrder();
                String header="";
                System.out.println("indexColumns.size()"+indexColumns.size());
                for(Integer j=0; j<indexColumns.size(); j++){
                    header+=(indexColumns.get(j).toString() + indexColumnOrder.get(j).toString()+",");
                }
                writer.append(header.substring(0, header.length()-1)+"\n");
                List<DbIndex.DbIndexEntry> idxData = idx.getIndexData();
                writer.append(String.valueOf(idxData.size())+"\n");
                for(DbIndex.DbIndexEntry dbe : idxData){
                    writer.append(dbe.getRID() + "," + dbe.getKey() + "\n");
                }
                writer.close();
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e){
            System.out.println("There was an exception!" + e.getMessage() + e.getCause());
        }
    }
}
