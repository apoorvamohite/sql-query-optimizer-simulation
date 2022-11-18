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
        // Single table only
        if(sql.getJoins()==null){
            DbIndex.getAllIndexes(((Table)sql.getFromItem()).getName(), true);
        } else { // Two table join
            List<String> tableNameList = new ArrayList<String>();
            tableNameList.add(((Table)sql.getFromItem()).getName());
            for(Join join: sql.getJoins()){
                tableNameList.add(((Table)join.getRightItem()).getName());
            }
            DbIndex.getAllIndexes(String.join("|", tableNameList), true);
        }
    }
}
