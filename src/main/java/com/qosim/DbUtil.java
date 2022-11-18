package com.qosim;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
//import io.bretty.console.table.Table;
import io.bretty.console.table.ColumnFormatter;
import io.bretty.console.table.Alignment;
import net.sf.jsqlparser.statement.create.index.CreateIndex;
import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;

/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
/**
 *
 * @author apoor
 */
public class DbUtil {

    protected static List<String> readFileLines(String filePath) {
        List<String> fileLines = new ArrayList<String>();
        try {
            File file = new File(filePath);
            Scanner sc = new Scanner(file);     //file to be scanned  
            while (sc.hasNextLine()) //returns true if and only if scanner has another token  
            {
                fileLines.add(sc.nextLine());
            }
            return fileLines;
        } catch (Exception e) {
            System.out.println("There was an error reading the file "+ filePath + ":"+ e);
            return null;
        }
    }

    protected static void formatTable(List<List<String>> arr) {
        int[] lengths = new int[arr.get(0).size()];
        
        for (Integer i=0; i<arr.size(); i++) {
            for(Integer j=0; j<arr.get(i).size(); j++){
                lengths[j] = Math.max(lengths[j], arr.get(i).get(j).length());
            }
        }
        Integer i = 0;
        for (List<String> row : arr) {
            i=0;
            for (String val : row) {
                System.out.print(centerString(lengths[i++], val) + "|");
            }
            System.out.print("\n");
        }
    }

    //https://stackoverflow.com/questions/8154366/how-to-center-a-string-using-string-format
    public static String centerString(int width, String s) {
        return String.format("%-" + width + "s", String.format("%" + (s.length() + (width - s.length()) / 2) + "s", s));
    }

    public static String leftJustifyString(int width, String s) {
        return String.format("%-" + width + "s", s);
    }

    public static String rightJustifyString(int width, String s) {
        return String.format("%" + width + "s", s);
    }

    public static String complementString(String str) {
        StringBuilder sb = new StringBuilder();
        for (Character c : str.toCharArray()) {
            sb.append((char) (127 - c));
        }
        return sb.toString();
    }

    public static String complementInteger(String n) {
        Integer num = Integer.parseInt(n);
        return String.valueOf(999999999 - num);
    }

    public static String preprocessSql(String sql) {
        sql = sql.replaceAll("(T[0-9]+\\.)([0-9])+", "$1_$2");
        sql = sql.replaceAll("([0-9]+[ADad])", "_$1");
        return sql;
    }

    public static void postprocessSql(CreateIndex ci) {
        List<String> finalColumnNames = new ArrayList<String>();
        for (String colName : ci.getIndex().getColumnsNames()) {
            finalColumnNames.add(colName.replace("_", ""));
        }
        ci.getIndex().setColumnsNames(finalColumnNames);
    }

    public static void postprocessSql(PlainSelect sel) {
        // Select items
        for (SelectItem selItem : sel.getSelectItems()) {
            Column col = (Column) ((SelectExpressionItem) selItem).getExpression();
            col.setColumnName(col.getColumnName().replace("_", ""));
            if (col.getTable() != null) {
                col.getTable().setName(col.getTable().getName().replace("_", ""));
            }
        }
        // From item
        Table fromTable = (Table) sel.getFromItem(Table.class);
        fromTable.setName(fromTable.getName().replace("_", ""));
        // Join Items
        if (sel.getJoins() != null) {
            for (Join join : sel.getJoins()) {
                Table rightTable = (Table) join.getRightItem();
                rightTable.setName(rightTable.getName().replace("_", ""));
                if (join.getOnExpressions() != null) {
                    for (Expression ex : join.getOnExpressions()) {
                        recursiveProcessing(ex);
                    }
                }
            }
        }
        // Predicates
        if (sel.getWhere() != null) {
            Expression ex = sel.getWhere();
            recursiveProcessing(ex);
        }
    }

    private static void recursiveProcessing(Object ex) {
        if (ex instanceof AndExpression) {
            AndExpression and = (AndExpression) ex;
            recursiveProcessing(and.getLeftExpression());
            recursiveProcessing(and.getRightExpression());
        } else if (ex instanceof OrExpression) {
            OrExpression or = (OrExpression) ex;
            recursiveProcessing(or.getLeftExpression());
            recursiveProcessing(or.getRightExpression());
        } else if (ex instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo) ex;
            recursiveProcessing(eq.getLeftExpression());
            recursiveProcessing(eq.getRightExpression());
        } else if (ex instanceof Column) {
            Column col = (Column) ex;
            col.setColumnName(col.getColumnName().replace("_", ""));
        } else if (ex instanceof InExpression) {
            InExpression in = (InExpression) ex;
            recursiveProcessing(in.getLeftExpression());
            for(Expression expr : ((ExpressionList)in.getRightItemsList()).getExpressions()){
                recursiveProcessing(expr);
            }
            //recursiveProcessing(in.getRightExpression());
        } else {
            System.out.println("==============Instance of============" + ex.getClass());
            return;
        }
    }
}
