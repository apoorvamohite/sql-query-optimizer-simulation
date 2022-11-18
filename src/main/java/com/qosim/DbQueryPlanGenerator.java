/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.qosim;

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;

import java.util.*;

/**
 *
 * @author apoor
 */
public class DbQueryPlanGenerator {
    
    public static void generatePlan(PlainSelect sql){
        DbQueryPlanTable planTable = new DbQueryPlanTable();
        DbQueryPredicateTable predicateTable = new DbQueryPredicateTable();
        // Single table only
        if(sql.getJoins()==null){
            List<DbIndex> indexes = DbIndex.getAllIndexes(((Table)sql.getFromItem()).getName(), true);
            // No Index
            if(indexes.isEmpty()){
                // No Predicate
                if(sql.getWhere() == null){
                    singleTableNoIndexNoPredicate(sql, planTable, predicateTable);
                } else { // Has Predicate
                    
                }
            } else { // Index present
                
            }
        } else { // Two table join
            List<String> tableNameList = new ArrayList<String>();
            tableNameList.add(((Table)sql.getFromItem()).getName());
            for(Join join: sql.getJoins()){
                tableNameList.add(((Table)join.getRightItem()).getName());
            }
            DbIndex.getAllIndexes(String.join("|", tableNameList), true);
        }
    }
    
    private static void singleTableNoIndexNoPredicate(PlainSelect sql, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable){
        DbTable tableStats = new DbTable(((Table)sql.getFromItem()).getName(), true);
        planTable.AccessType = 'R';
        planTable.MatchCols = 0;
        planTable.IndexOnly = 'N';
        planTable.Prefetch = "S";
        
        if(sql.getOrderByElements()!=null){
            planTable.SortC_OrderBy = 'Y';
        } else {
            planTable.SortC_OrderBy = 'N';
        }
        planTable.Table1Card = tableStats.getNumRows();
        
        DbQueryPlanTable.printTable(planTable);
    }
}
