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
import java.util.ArrayList;

/**
 *
 * @author apoor
 */
public class Qosim {

    public static void main(String[] args) {
        List<String> statements = DbUtil.readFileLines(args[0]);
        for (String sql : statements) {
            try {
                sql = DbUtil.preprocessSql(sql);
                Statement stmt = (Statement) CCJSqlParserUtil.parse(sql);
                String className = stmt.getClass().getSimpleName();
                switch (className) {
                    case "CreateIndex":
                        // TODO: Formatting
                        CreateIndex ci = (CreateIndex) stmt;
                        DbUtil.postprocessSql(ci);
                        createIndex(ci);
                        break;
                    case "Select":
                        PlainSelect sel = (PlainSelect) ((Select) stmt).getSelectBody();
                        System.out.println("Before PostProcess: "+sel);
                        DbUtil.postprocessSql(sel);
                        System.out.println("After PostProcess: "+sel);
                        DbQueryPlanGenerator.generatePlan(sel);
                        break;
                    case "Drop":
                        Drop drop = (Drop) stmt;
                        dropIndex(drop);
                        break;
                    default:
                        System.out.println("this is default");
                }
            } catch (JSQLParserException pex) {
                if (sql.toUpperCase().startsWith("LIST INDEX")) {
                    String tableName = sql.split(" ")[2].replace("_", "");
                    DbIndex.getAllIndexes(tableName, false);
                }
            } catch (Exception e) {
                throw e;
            }
            System.out.println("==========================================================================================================================");
        }
    }

    protected static void dropIndex(Drop drop) {
        String fileName = drop.getName().getName();
        File idxFile = new File(DbConstants.DB_INDEXES_DIR_PATH + fileName + DbConstants.DB_INDEX_FILE_EXT);
        if (idxFile.delete()) {
            System.out.println("DROP INDEX SUCCESS: " + fileName);
        } else {
            System.out.println("DROP INDEX FAILED");
        }
    }

    protected static void createIndex(CreateIndex ci) {
        String fileName = ci.getTable().getName() + ci.getIndex().getName();
        File idxFile = new File(DbConstants.DB_INDEXES_DIR_PATH + fileName + DbConstants.DB_INDEX_FILE_EXT);
        DbIndex idx = new DbIndex(ci.getTable().getName(), ci.getIndex().getColumnsNames());
        try {
            if (idxFile.createNewFile()) {
                FileWriter writer = new FileWriter(idxFile);
                List<Integer> indexColumns = idx.getIndexColumns();
                List<Character> indexColumnOrder = idx.getIndexColumnOrder();
                String header = "";
                for (Integer j = 0; j < indexColumns.size(); j++) {
                    header += (indexColumns.get(j).toString() + indexColumnOrder.get(j).toString() + ",");
                }
                writer.append(header.substring(0, header.length() - 1) + "\n");
                List<DbIndex.DbIndexEntry> idxData = idx.getIndexData();
                writer.append(String.valueOf(idxData.size()) + "\n");
                for (DbIndex.DbIndexEntry dbe : idxData) {
                    writer.append(dbe.getRID() + " '" + dbe.getKey() + "'\n");
                }
                writer.close();
            } else {
                System.out.println("File already exists.");
            }
        } catch (IOException e) {
            System.out.println("There was an exception!" + e.getMessage() + e.getCause());
        }
    }
}
