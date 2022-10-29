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
import java.util.Scanner;
import java.util.ArrayList;
import java.io.FilenameFilter;

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
                System.out.println("className");
                String className = stmt.getClass().getSimpleName();
                System.out.println("className" + className);
                switch (className) {
                    case "CreateIndex":
                        System.out.println(className);
                        CreateIndex ci = (CreateIndex) stmt;
                        System.out.println(className);
                        System.out.println(ci.getIndex());
                        System.out.println(ci.getTable());
                        System.out.println(ci.getIndex().getColumns());
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
                System.out.println("There was an exception!" + e.getMessage() + e.getCause());
            }
        }

    }
}
