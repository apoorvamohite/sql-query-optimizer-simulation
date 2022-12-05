/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.qosim;

import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.schema.Column;
/**
 *
 * @author apoor
 */
public class DbQueryPredicateTableRow {
    public String PredicateNum;
    public char Type;
    public int ColumnCardinality1;
    public int ColumnCardinality2;
    public double FilterFactor1;
    public double FilterFactor2;
    public int Sequence;
    public String Text;
    private Expression predicate;
    private String Table1;
    private String Table2;
    
    public Expression getPredicate(){
        return this.predicate;
    }
    
    public String getTable1(){
        return this.Table1;
    }
    
    public String getTable2(){
        return this.Table2;
    }
    
    public DbQueryPredicateTableRow(Expression ex, Integer predicateNum, DbTable tableStats){
        if(ex instanceof EqualsTo){
            EqualsTo eq = (EqualsTo) ex;
            this.predicate = eq;
            this.PredicateNum = "PredNo"+predicateNum;
            this.Type = 'E';
            this.ColumnCardinality1 = tableStats.getColumnCardinality(Integer.parseInt(((Column)eq.getLeftExpression()).getColumnName()));
            this.FilterFactor1 = 1.0/this.ColumnCardinality1;
            this.Text = eq.toString();
            this.Table1 = ((Column)eq.getLeftExpression()).getTable().getName();
        } else if(ex instanceof GreaterThan){
            GreaterThan eq = (GreaterThan) ex;
            this.predicate = eq;
            this.PredicateNum = "PredNo"+predicateNum;
            this.Type = 'R';
            this.ColumnCardinality1 = tableStats.getColumnCardinality(Integer.parseInt(((Column)eq.getLeftExpression()).getColumnName()));
            Integer value = Integer.parseInt(((LongValue)eq.getRightExpression()).getStringValue());
            Integer columnNumber = Integer.parseInt(((Column)eq.getLeftExpression()).getColumnName());
            Double highKey = tableStats.getHighKey(columnNumber).doubleValue();
            Double lowKey = tableStats.getLowKey(columnNumber).doubleValue();
            if(value.doubleValue()<lowKey || value.doubleValue()>highKey){
                this.FilterFactor1 = 1.0;
            } else {
                this.FilterFactor1 = (highKey - value.doubleValue())/(highKey - lowKey + 1.0);
            }
            this.Text = eq.toString();
            this.Table1 = ((Column)eq.getLeftExpression()).getTable().getName();
        } else if(ex instanceof MinorThan){
            MinorThan eq = (MinorThan) ex;
            this.predicate = eq;
            this.PredicateNum = "PredNo"+predicateNum;
            this.Type = 'R';
            this.ColumnCardinality1 = tableStats.getColumnCardinality(Integer.parseInt(((Column)eq.getLeftExpression()).getColumnName()));
            Integer value = Integer.parseInt(((LongValue)eq.getRightExpression()).getStringValue());
            Integer columnNumber = Integer.parseInt(((Column)eq.getLeftExpression()).getColumnName());
            Double highKey = tableStats.getHighKey(columnNumber).doubleValue();
            Double lowKey = tableStats.getLowKey(columnNumber).doubleValue();
            if(value.doubleValue()<lowKey || value.doubleValue()>highKey){
                this.FilterFactor1 = 1.0;
            } else {
                this.FilterFactor1 = (value.doubleValue() - lowKey)/(highKey - lowKey + 1.0);
            }
            this.Text = eq.toString();
            this.Table1 = ((Column)eq.getLeftExpression()).getTable().getName();
        } else if(ex instanceof OrExpression){
            OrExpression or = (OrExpression) ex;
            this.predicate = or;
            this.PredicateNum = "PredNo"+predicateNum;
            this.Type = 'I';
            this.ColumnCardinality1 = tableStats.getColumnCardinality(Integer.parseInt(((Column)((EqualsTo)or.getRightExpression()).getLeftExpression()).getColumnName()));
            this.FilterFactor1 = (double)calculateInListFF(or)/this.ColumnCardinality1;
            this.Text = or.toString();
            this.Table1 = ((Column)((EqualsTo)or.getRightExpression()).getLeftExpression()).getTable().getName();
        } else if(ex instanceof InExpression){
            InExpression in = (InExpression) ex;
            this.predicate = in;
            this.PredicateNum = "PredNo"+predicateNum;
            this.Type = 'I';
            this.Text = in.toString();
            this.Table1 = ((Column)in.getLeftExpression()).getTable().getName();
            this.ColumnCardinality1 = tableStats.getColumnCardinality(Integer.parseInt(((Column)in.getLeftExpression()).getColumnName()));
            this.FilterFactor1 = ((double)(((ExpressionList)in.getRightItemsList()).getExpressions().size()))/this.ColumnCardinality1;
        }
    }
    
    private static Integer calculateInListFF(Expression ex){
        if(ex instanceof EqualsTo){
            return 1;
        } else if (ex instanceof OrExpression){
            return calculateInListFF(((OrExpression)ex).getLeftExpression()) + calculateInListFF(((OrExpression)ex).getRightExpression());
        } else {
            return 0;
        }
    }
    
    
    public DbQueryPredicateTableRow(Expression ex, Integer predicateNum, DbTable table1Stats, DbTable table2Stats){
        if(ex instanceof EqualsTo){
            EqualsTo eq = (EqualsTo) ex;
            this.predicate = eq;
            this.PredicateNum = "PredNo"+predicateNum;
            this.Type = 'E';
            this.ColumnCardinality1 = table1Stats.getColumnCardinality(Integer.parseInt(((Column)eq.getLeftExpression()).getColumnName()));
            this.ColumnCardinality2 = table2Stats.getColumnCardinality(Integer.parseInt(((Column)eq.getRightExpression()).getColumnName()));
            this.FilterFactor1 = 1.0/this.ColumnCardinality1;
            this.FilterFactor2 = 1.0/this.ColumnCardinality2;
            this.Text = eq.toString();
            this.Table1 = ((Column)eq.getLeftExpression()).getTable().getName();
            this.Table2 = ((Column)eq.getRightExpression()).getTable().getName();
        }
    }
}
