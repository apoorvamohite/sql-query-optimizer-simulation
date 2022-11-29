/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.qosim;

import java.util.List;
import java.util.ArrayList;
import org.sk.PrettyTable;

/**
 *
 * @author apoor
 */
public class DbQueryPredicateTable {

    public List<DbQueryPredicateTableRow> rows;

    public DbQueryPredicateTable() {
        this.rows = new ArrayList<DbQueryPredicateTableRow>();
    }
    
    public void setPredicateSequenceFF1(){
        int min = 0;
        int sequenceStart = 0;
        for(int i=0; i<this.rows.size(); i++){
            if(this.rows.get(i).Sequence > sequenceStart){
                sequenceStart = this.rows.get(i).Sequence;
            }
        }
        sequenceStart++;
        for(int i=1; i<=this.rows.size(); i++){
            if(this.rows.get(i-1).FilterFactor1 <= this.rows.get(min).FilterFactor1){
                min = i-1;
            }
            int max = min;
            for(int j=this.rows.size()-1; j>=0; j--){
                if(this.rows.get(j).Sequence == 0 && this.rows.get(j).FilterFactor1>=this.rows.get(max).FilterFactor1){
                    max = j;
                }
            }
            if(this.rows.get(max).Sequence == 0)
                this.rows.get(max).Sequence = sequenceStart++;
        }
    }
    
    public void setPredicateSequenceFF1ByTable(String tableName){
        int min = 0;
        int sequenceStart = 0;
        for(int i=0; i<this.rows.size(); i++){
            if(this.rows.get(i).Sequence > sequenceStart){
                sequenceStart = this.rows.get(i).Sequence;
            }
        }
        sequenceStart++;
        for(int i=1; i<=this.rows.size(); i++){
            if(this.rows.get(i-1).FilterFactor1 <= this.rows.get(min).FilterFactor1 && this.rows.get(i-1).getTable1().equals(tableName)){
                min = i-1;
            }
            int max = min;
            for(int j=this.rows.size()-1; j>=0; j--){
                if(this.rows.get(j).Sequence == 0 && this.rows.get(j).FilterFactor1>=this.rows.get(max).FilterFactor1  && this.rows.get(j).getTable1().equals(tableName)){
                    max = j;
                }
            }
            if(this.rows.get(max).Sequence == 0  && this.rows.get(max).getTable1().equals(tableName))
                this.rows.get(max).Sequence = sequenceStart++;
        }
    }

    public static void printTable(DbQueryPredicateTable predTable) {
        String desc = "Description – Possible values\n"
                + "Type – E (equal), R (Range), I (IN List)\n"
                + "C1 – column cardinality left hand side (-1 if table is empty)\n"
                + "C2 – column cardinality right hand side (join predicate only)\n"
                + "FF 1 - Estimate FF for left hand side (-1 if table is empty)\n"
                + "FF 2 - Estimate FF for right hand side (join predicate only)\n"
                + "Seq – either 1,2,3,4,5;  the order of each predicate is being evaluated\n"
                + "Text – the original text of the predicate";

        PrettyTable table = new PrettyTable("Predicate Table", "Type", "C1", "C2", "FF1", "FF2", "Seq", "Text");
        for(DbQueryPredicateTableRow row: predTable.rows){
            table.addRow(
                    String.valueOf(row.PredicateNum), 
                    String.valueOf(row.Type), 
                    String.valueOf(row.ColumnCardinality1),
                    String.valueOf(row.ColumnCardinality2),
                    String.valueOf(row.FilterFactor1),
                    String.valueOf(row.FilterFactor2),
                    String.valueOf(row.Sequence),
                    String.valueOf(row.Text)
            );
        }
        System.out.println(table);
    }
}
