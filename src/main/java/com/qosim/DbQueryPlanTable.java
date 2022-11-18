/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.qosim;

import org.sk.PrettyTable;

/**
 *
 * @author apoor
 */
public class DbQueryPlanTable {

    public int QueryBlockNo = 1;
    public char AccessType = 'R';
    public int MatchCols = 0;
    public String AccessName="";
    public char IndexOnly;
    public String Prefetch="";
    public char SortC_OrderBy;
    public int Table1Card = -1;
    public int Table2Card = -1;
    public String LeadingTable="";

    public static void printTable(DbQueryPlanTable planTable) {
        
        PrettyTable table = new PrettyTable("Plan Table", "Value", "Description – Possible values");
        table.addRow("QBlockNo", String.valueOf(planTable.QueryBlockNo), "Always 1 since we have only 1 block");
        table.addRow("AccessType", String.valueOf(planTable.AccessType), "R - TS scan; I - Index Scan; N - IN list index scan");
        table.addRow("MatchCols", String.valueOf(planTable.MatchCols), "Number of matched columns in the INDEX key where ACCESSTYPE is I or N");
        table.addRow("AccessName", planTable.AccessName, "Name of index file if ACCESSTYPE is I or N");
        table.addRow("IndexOnly", String.valueOf(planTable.IndexOnly), "Y or N");
        table.addRow("Prefetch", planTable.Prefetch, "Blank - no prefetch; S - sequential prefetch");
        table.addRow("SortC_OrderBy", String.valueOf(planTable.SortC_OrderBy), "Y or N");
        table.addRow("Table1Card", String.valueOf(planTable.Table1Card), "Table 1 Cardinality");
        table.addRow("Table2Card", String.valueOf(planTable.Table2Card), "Table 2 Cardinality");
        table.addRow("LeadingTable", planTable.LeadingTable, "Table name of the outer table in NLJ");
        System.out.println(table);
    }
}
