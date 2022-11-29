/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package com.qosim;

import net.sf.jsqlparser.statement.select.*;
import net.sf.jsqlparser.schema.*;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.conditional.*;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;

import java.util.*;

/**
 *
 * @author apoor
 */
public class DbQueryPlanGenerator {

    public static void generatePlan(PlainSelect sql) {
        DbQueryPlanTable planTable = new DbQueryPlanTable();
        DbQueryPredicateTable predicateTable = new DbQueryPredicateTable();
        // Single table only
        if (sql.getJoins() == null) {
            List<DbIndex> indexes = DbIndex.getAllIndexes(((Table) sql.getFromItem()).getName(), true);
            // No Index
            if (indexes.isEmpty()) {
                // No Predicate
                if (sql.getWhere() == null) {
                    singleTableNoIndexNoPredicate(sql, planTable, predicateTable);
                } else { // Has Predicate
                    singleTableNoIndexWithPredicate(sql, planTable, predicateTable);
                }
            } else { // Index present
                if (sql.getWhere() == null) {
                    singleTableWithIndexNoPredicate(sql, indexes, planTable, predicateTable);
                } else {
                    singleTableWithIndexWithPredicate(sql, indexes, planTable, predicateTable);
                }
            }
        } else { // Two table join
            List<String> tableNameList = new ArrayList<String>();
            tableNameList.add(((Table) sql.getFromItem()).getName());
            for (Join join : sql.getJoins()) {
                tableNameList.add(((Table) join.getRightItem()).getName());
            }
            List<DbIndex> indexes = DbIndex.getAllIndexes(String.join("|", tableNameList), true);
            // No Index
            if (indexes.isEmpty()) {
                // No Predicate
                if (sql.getWhere() == null) {
                    System.out.println("HAS JOIN ON" + sql.getJoins() + sql.getFromItem());
                    for (Expression joinExp : sql.getJoins().get(0).getOnExpressions()) {
                        twoTableNoIndexNoPredicate(sql, planTable, predicateTable, joinExp);
                    }
                } else {
                    EqualsTo eq = (EqualsTo) getJoinPredicate(sql.getWhere());
                    List<Expression> predicateList = new ArrayList<Expression>();
                    getPredicateList(sql.getWhere(), predicateList);
                    if (predicateList.size() <= 1) {
                        // No Local Predicates
                        twoTableNoIndexNoPredicate(sql, planTable, predicateTable, eq);
                    } else {
                        // With Local Predicates
                        twoTableNoIndexWithPredicate(sql, planTable, predicateTable, eq);
                    }
                }
            } else {
                System.out.println("HAS INDEX JOIN PREDICATE" + sql.getJoins().get(0).getRightItem());
            }
        }
    }

    private static void twoTableNoIndexWithPredicate(PlainSelect sql, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable, Expression joinExp) {
        String t1 = ((Column) (((EqualsTo) joinExp).getLeftExpression())).getTable().getName();
        String t2 = ((Column) (((EqualsTo) joinExp).getRightExpression())).getTable().getName();
        DbTable table1Stats = new DbTable(t1, true);
        DbTable table2Stats = new DbTable(t2, true);
        planTable.AccessType = 'R';
        planTable.MatchCols = 0;
        planTable.IndexOnly = 'N';
        planTable.Prefetch = "S";

        if (sql.getOrderByElements() != null) {
            planTable.SortC_OrderBy = 'Y';
        } else {
            planTable.SortC_OrderBy = 'N';
        }
        planTable.Table1Card = table1Stats.getNumRows();
        planTable.Table2Card = table2Stats.getNumRows();

        getPredicates(sql, table1Stats, table2Stats, predicateTable);

        Double t1ff = 1.0;
        Double t2ff = 1.0;
        for (DbQueryPredicateTableRow row : predicateTable.rows) {
            if (row.Type == 'E') {
                EqualsTo eq = (EqualsTo) row.getPredicate();
                if (!(eq.getRightExpression() instanceof Column)) {
                    if (((Column) eq.getLeftExpression()).getTable().getName().equals(t1)) {
                        t1ff *= row.FilterFactor1;
                    } else {
                        t2ff *= row.FilterFactor1;
                    }
                }
            } else if (row.getPredicate() instanceof GreaterThan) {
                GreaterThan gt = (GreaterThan) row.getPredicate();
                if (((Column) gt.getLeftExpression()).getTable().getName().equals(t1)) {
                    t1ff *= row.FilterFactor1;
                } else {
                    t2ff *= row.FilterFactor1;
                }
            } else if (row.getPredicate() instanceof MinorThan) {
                MinorThan mt = (MinorThan) row.getPredicate();
                if (((Column) mt.getLeftExpression()).getTable().getName().equals(t1)) {
                    t1ff *= row.FilterFactor1;
                } else {
                    t2ff *= row.FilterFactor1;
                }
            }
        }
        if (t1ff < t2ff) {
            planTable.LeadingTable = t1;
            planTable.InnerTable = t2;
        } else {
            planTable.LeadingTable = t2;
            planTable.InnerTable = t1;
        }
        predicateTable.setPredicateSequenceFF1ByTable(planTable.LeadingTable);
        // Should the join predicate sequence be last? SELECT T2.2, T3.2 FROM T2, T3 WHERE T2.2 = T3.2 AND T2.1='ABC' AND T3.1='XYZ' AND T3.2=15
        predicateTable.setPredicateSequenceFF1ByTable(planTable.InnerTable);

        DbQueryPlanTable.printTable(planTable);
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static Expression getJoinPredicate(Expression ex) {
        if (ex instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo) ex;
            if (eq.getLeftExpression() instanceof Column && eq.getRightExpression() instanceof Column) {
                return eq;
            }
        } else if (ex instanceof AndExpression) {
            AndExpression and = (AndExpression) ex;
            Expression left = getJoinPredicate(and.getLeftExpression());
            Expression right = getJoinPredicate(and.getRightExpression());
            return (left == null ? (right == null ? null : right) : left);
        } else if (ex instanceof OrExpression) {
            OrExpression or = (OrExpression) ex;
            Expression left = getJoinPredicate(or.getLeftExpression());
            Expression right = getJoinPredicate(or.getRightExpression());
            return (left == null ? (right == null ? null : right) : left);
        }
        return null;
    }

    private static void twoTableNoIndexNoPredicate(PlainSelect sql, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable, Expression joinExp) {
        DbTable table1Stats = new DbTable(((Table) sql.getFromItem()).getName(), true);
        DbTable table2Stats = new DbTable(((Table) sql.getJoins().get(0).getRightItem()).getName(), true);
        planTable.AccessType = 'R';
        planTable.MatchCols = 0;
        planTable.IndexOnly = 'N';
        planTable.Prefetch = "S";

        if (sql.getOrderByElements() != null) {
            planTable.SortC_OrderBy = 'Y';
        } else {
            planTable.SortC_OrderBy = 'N';
        }
        planTable.Table1Card = table1Stats.getNumRows();
        planTable.Table2Card = table2Stats.getNumRows();
        planTable.LeadingTable = table1Stats.getNumRows() >= table2Stats.getNumRows() ? table1Stats.getTableName() : table2Stats.getTableName();

        DbQueryPlanTable.printTable(planTable);
        predicateTable.rows.add(new DbQueryPredicateTableRow(joinExp, 1, table1Stats, table2Stats));
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static void singleTableWithIndexNoPredicate(PlainSelect sql, List<DbIndex> indexes, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable) {
        String tableName = ((Table) sql.getFromItem()).getName();
        DbTable tableStats = new DbTable(tableName, true);
        Set<String> columnNames = new HashSet<String>();
        for (SelectItem si : sql.getSelectItems()) {
            Column column = (Column) ((SelectExpressionItem) si).getExpression();
            columnNames.add(column.getColumnName());
        }
        int curMatch = 0;
        int maxMatch = 0;
        DbIndex bestIdx = null;
        for (DbIndex idx : indexes) {
            curMatch = 0;
            for (Integer col : idx.getIndexColumns()) {
                if (columnNames.contains(col.toString())) {
                    curMatch++;
                }
            }
            if (curMatch > maxMatch) {
                maxMatch = curMatch;
                bestIdx = idx;
            }
        }

        planTable.AccessType = maxMatch > 0 ? 'I' : 'R';
        planTable.MatchCols = bestIdx != null ? maxMatch : 0;
        planTable.AccessName = bestIdx != null ? bestIdx.getIndexName() : "";
        planTable.IndexOnly = bestIdx != null && maxMatch == bestIdx.getNumIndexColumns() ? 'Y' : 'N';
        planTable.Prefetch = bestIdx != null && maxMatch != bestIdx.getNumIndexColumns() ? "S" : "";

        if (sql.getOrderByElements() != null) {
            planTable.SortC_OrderBy = 'Y';
        } else {
            planTable.SortC_OrderBy = 'N';
        }
        planTable.Table1Card = tableStats.getNumRows();
        DbQueryPlanTable.printTable(planTable);
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static void singleTableWithIndexWithPredicate(PlainSelect sql, List<DbIndex> indexes, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable) {
        String tableName = ((Table) sql.getFromItem()).getName();
        DbTable tableStats = new DbTable(tableName, true);
        if (sql.getWhere() instanceof AndExpression || sql.getWhere() instanceof EqualsTo) {
            List<Expression> predicateList = new ArrayList<>();
            System.out.println(sql.getWhere().getClass());
            getPredicateList(sql.getWhere(), predicateList);
            DbIndex bestIdx = evaluateBestIndex(indexes, predicateList, planTable);

            planTable.AccessType = bestIdx != null ? 'I' : 'R';
            planTable.AccessName = bestIdx != null ? bestIdx.getIndexName() : "";

            int maxMatch = 0;
            Set<String> columnNames = new HashSet<String>();
            if (bestIdx != null) {
                for (SelectItem si : sql.getSelectItems()) {
                    Column column = (Column) ((SelectExpressionItem) si).getExpression();
                    columnNames.add(column.getColumnName());
                }
                for (Integer col : bestIdx.getIndexColumns()) {
                    if (columnNames.contains(col.toString())) {
                        maxMatch++;
                    }
                }
            }

            planTable.IndexOnly = bestIdx != null && columnNames.size() <= bestIdx.getNumIndexColumns() && maxMatch == columnNames.size() ? 'Y' : 'N';
            planTable.Prefetch = planTable.IndexOnly == 'N' ? "S" : "";

            if (sql.getOrderByElements() != null) {
//                for(OrderByElement obe : sql.getOrderByElements()){
//                    if(bestIdx.getIndexColumns().contains(Integer.valueOf(((Column)obe.getExpression()).getColumnName()) )){
//                        
//                    }
//                }
                planTable.SortC_OrderBy = 'Y';
            } else {
                planTable.SortC_OrderBy = 'N';
            }
            planTable.Table1Card = tableStats.getNumRows();

            DbQueryPlanTable.printTable(planTable);

            setPredicateTable(bestIdx, predicateList, predicateTable, tableStats);
            DbQueryPredicateTable.printTable(predicateTable);
        } else if (sql.getWhere() instanceof OrExpression) {
            // TODO
        }
    }

    private static void setPredicateTable(DbIndex idx, List<Expression> predicateList, DbQueryPredicateTable predicateTable, DbTable tableStats) {
        int predNum = 1;
        int sequence = 1;
        Expression cur = null;
        for (Integer column : idx.getIndexColumns()) {
            for (Expression ex : predicateList) {
                if (ex instanceof EqualsTo) {
                    EqualsTo eq = (EqualsTo) ex;
                    if (((Column) eq.getLeftExpression()).getColumnName().equals(column.toString())) {
                        DbQueryPredicateTableRow row = new DbQueryPredicateTableRow(ex, predNum++, tableStats);
                        row.Sequence = sequence++;
                        predicateTable.rows.add(row);
                        cur = ex;
                        break;
                    }
                } else if (ex instanceof GreaterThan) {
                    Column c = (Column) ((GreaterThan) ex).getLeftExpression();
                    if (c.getColumnName().equals(column.toString())) {
                        DbQueryPredicateTableRow row = new DbQueryPredicateTableRow(ex, predNum++, tableStats);
                        row.Sequence = sequence++;
                        predicateTable.rows.add(row);
                        cur = ex;
                        break;
                    }
                } else if (ex instanceof MinorThan) {
                    Column c = (Column) ((MinorThan) ex).getLeftExpression();
                    if (c.getColumnName().equals(column.toString())) {
                        DbQueryPredicateTableRow row = new DbQueryPredicateTableRow(ex, predNum++, tableStats);
                        row.Sequence = sequence++;
                        predicateTable.rows.add(row);
                        cur = ex;
                        break;
                    }
                }
            }
            predicateList.remove(cur);
        }
        for (Expression ex : predicateList) {
            predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNum++, tableStats));
        }
        predicateTable.setPredicateSequenceFF1();
    }

    private static DbIndex evaluateBestIndex(List<DbIndex> indexes, List<Expression> predicateList, DbQueryPlanTable planTable) {
        Map<String, HashMap<String, Integer>> predicateMap = new HashMap<String, HashMap<String, Integer>>();

        System.out.println("predicateList------" + predicateList);
        for (DbIndex idx : indexes) {
            List<Integer> predicateEvaluation = new ArrayList<>(); // 0=EQUALS, 1=GREATER_THAN, -1=MINOR_THAN
            predicateEvaluation.add(0);
            System.out.println("----------------index--------------------------" + idx.getIndexName());
            System.out.println("columns------" + idx.getIndexColumns());
            HashMap<String, Integer> idxMap = new HashMap<String, Integer>();
            char phase = 'M';
            for (Integer column : idx.getIndexColumns()) {
                for (Expression ex : predicateList) {
                    if (ex instanceof EqualsTo) {
                        EqualsTo eq = (EqualsTo) ex;
                        if (((Column) eq.getLeftExpression()).getColumnName().equals(column.toString()) && phase == 'M') {
                            if (predicateEvaluation.get(predicateEvaluation.size() - 1) == 0) {
                                System.out.println(column + "===Equals M*********" + ex);
                                idxMap.put("MATCHING", idxMap.getOrDefault("MATCHING", 0) + 1);
                                System.out.println("Adding to predeval");
                                predicateEvaluation.add(0);
                                break;
                            } else if (predicateEvaluation.get(predicateEvaluation.size() - 1) != 0) { // Screening
                                phase = 'S';
                                idxMap.put("SCREENING", idxMap.getOrDefault("SCREENING", 0) + 1);
                                System.out.println(column + "===Equals S*********" + ex);
                                predicateEvaluation.add(0);
                                break;
                            }
                        } else if (((Column) eq.getLeftExpression()).getColumnName().equals(column.toString()) && phase == 'S') {
                            System.out.println(column + "===Equals S*********" + ex);
                            idxMap.put("SCREENING", idxMap.getOrDefault("SCREENING", 0) + 1);
                            System.out.println("Adding to predeval");
                            predicateEvaluation.add(0);
                            break;
                        }
                    } else if (ex instanceof GreaterThan) {
                        Column c = (Column) ((GreaterThan) ex).getLeftExpression();
                        if (c.getColumnName().equals(column.toString()) && phase == 'M') {
                            if (predicateEvaluation.get(predicateEvaluation.size() - 1) == 0) {
                                System.out.println(column + "===GreaterThan M*********" + ex);
                                idxMap.put("MATCHING", idxMap.getOrDefault("MATCHING", 0) + 1);
                                predicateEvaluation.add(1);
                                break;
                            } else if (predicateEvaluation.get(predicateEvaluation.size() - 1) != 0) { // Screening
                                phase = 'S';
                                idxMap.put("SCREENING", idxMap.getOrDefault("SCREENING", 0) + 1);
                                System.out.println(column + "===GreaterThan S*********" + ex);
                                predicateEvaluation.add(1);
                                break;
                            }
                        } else if (c.getColumnName().equals(column.toString()) && phase == 'S') {
                            idxMap.put("SCREENING", idxMap.get("SCREENING") + 1);
                            predicateEvaluation.add(1);
                            break;
                        }
                    } else if (ex instanceof MinorThan) {
                        Column c = (Column) ((MinorThan) ex).getLeftExpression();
                        if (c.getColumnName().equals(column.toString()) && phase == 'M') {
                            if (predicateEvaluation.get(predicateEvaluation.size() - 1) == 0) {
                                idxMap.put("MATCHING", idxMap.getOrDefault("MATCHING", 0) + 1);
                                predicateEvaluation.add(-1);
                                break;
                            } else if (predicateEvaluation.get(predicateEvaluation.size() - 1) != 0) { // Screening
                                phase = 'S';
                                idxMap.put("SCREENING", idxMap.getOrDefault("SCREENING", 0) + 1);
                                predicateEvaluation.add(-1);
                                break;
                            }
                        } else if (c.getColumnName().equals(column.toString()) && phase == 'S') {
                            idxMap.put("SCREENING", idxMap.get("SCREENING") + 1);
                            predicateEvaluation.add(-1);
                            break;
                        }
                        predicateEvaluation.add(-1);
                    }
                }
                System.out.println("predicateEvaluation------" + predicateEvaluation);
            }
            predicateMap.put(idx.getIndexName(), idxMap);
        }
        DbIndex bestIndex = null;
        int maxMatch = 0;
        for (DbIndex idx : indexes) {
            HashMap<String, Integer> idxMap = predicateMap.get(idx.getIndexName());
            if (idxMap.getOrDefault("MATCHING", 0) > maxMatch) {
                maxMatch = idxMap.getOrDefault("MATCHING", 0);
                bestIndex = idx;
            } else if (idxMap.getOrDefault("MATCHING", 0) == maxMatch) {
                // TODO break tie with FF
                if (idxMap.getOrDefault("SCREENING", 0) > predicateMap.get(bestIndex.getIndexName()).getOrDefault("SCREENING", 0)) {
                    bestIndex = idx;
                }
            }
        }
        planTable.MatchCols = bestIndex != null ? predicateMap.get(bestIndex.getIndexName()).get("MATCHING") : 0;
        System.out.println("predicateMap" + predicateMap);
        return bestIndex;
    }

    private static void getPredicateList(Expression ex, List<Expression> result) {
        if (ex instanceof EqualsTo) {
            result.add((EqualsTo) ex);
        } else if (ex instanceof GreaterThan) {
            result.add((GreaterThan) ex);
        } else if (ex instanceof MinorThan) {
            result.add((MinorThan) ex);
        } else if (ex instanceof AndExpression) {
            getPredicateList(((AndExpression) ex).getLeftExpression(), result);
            getPredicateList(((AndExpression) ex).getRightExpression(), result);
        }
    }

    private static void singleTableNoIndexNoPredicate(PlainSelect sql, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable) {
        DbTable tableStats = new DbTable(((Table) sql.getFromItem()).getName(), true);
        planTable.AccessType = 'R';
        planTable.MatchCols = 0;
        planTable.IndexOnly = 'N';
        planTable.Prefetch = "S";

        if (sql.getOrderByElements() != null) {
            planTable.SortC_OrderBy = 'Y';
        } else {
            planTable.SortC_OrderBy = 'N';
        }
        planTable.Table1Card = tableStats.getNumRows();

        DbQueryPlanTable.printTable(planTable);
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static void singleTableNoIndexWithPredicate(PlainSelect sql, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable) {
        DbTable tableStats = new DbTable(((Table) sql.getFromItem()).getName(), true);

        planTable.AccessType = 'R';
        planTable.MatchCols = 0;
        planTable.IndexOnly = 'N';
        planTable.Prefetch = "S";

        if (sql.getOrderByElements() != null) {
            planTable.SortC_OrderBy = 'Y';
        } else {
            planTable.SortC_OrderBy = 'N';
        }
        planTable.Table1Card = tableStats.getNumRows();

        DbQueryPlanTable.printTable(planTable);

        // TODO: process OR predicates on same table that are in alternate order. SELECT T2.1 FROM T2 WHERE T2.1=1 OR T2.2=2 OR T2.1=3 OR T2.2=4
        getPredicates(sql, tableStats, null, predicateTable);
        predicateTable.setPredicateSequenceFF1();
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static void getPredicates(PlainSelect sql, DbTable table1Stats, DbTable table2Stats, DbQueryPredicateTable predicateTable) {
        predNo = 1;
        getPredicatesRecursive(sql.getWhere(), table1Stats, table2Stats, predicateTable);
    }

    private static Integer predNo = 1;

    private static void getPredicatesRecursive(Expression ex, DbTable table1Stats, DbTable table2Stats, DbQueryPredicateTable predicateTable) {
        if (ex instanceof EqualsTo) {
            // Equal Join predicate
            if (((EqualsTo) ex).getRightExpression() instanceof Column) {
                predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, table1Stats, table2Stats));
            } else { // Equal predicate
                predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, table1Stats));
            }
        } else if (ex instanceof NotEqualsTo) {
            predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, table1Stats));
        } else if (ex instanceof GreaterThan) {
            predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, table1Stats));
        } else if (ex instanceof MinorThan) {
            predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, table1Stats));
        } else if (ex instanceof AndExpression) {
            getPredicatesRecursive(((AndExpression) ex).getLeftExpression(), table1Stats, table2Stats, predicateTable);
            getPredicatesRecursive(((AndExpression) ex).getRightExpression(), table1Stats, table2Stats, predicateTable);
        } else if (ex instanceof OrExpression) {
            OrExpression or = (OrExpression) ex;
            // If parenthesis to be included change logic here
            if (or.getRightExpression() instanceof EqualsTo && canBeModifiedToInList(or, ((Column) ((EqualsTo) or.getRightExpression()).getLeftExpression()).getColumnName())) {
                predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, table1Stats));
            } else {
                getPredicatesRecursive(or.getLeftExpression(), table1Stats, table2Stats, predicateTable);
                getPredicatesRecursive(or.getRightExpression(), table1Stats, table2Stats, predicateTable);
            }
        }
    }

    private static Boolean canBeModifiedToInList(Expression ex, String columnName) {
        if (ex instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo) ex;
            if (((Column) eq.getLeftExpression()).getColumnName().equals(columnName)) {
                return true;
            } else if (((Column) eq.getLeftExpression()).getColumnName().equals(columnName)) {
                return false;
            }
        } else if (ex instanceof OrExpression) {
            Boolean left = canBeModifiedToInList(((OrExpression) ex).getLeftExpression(), columnName);
            Boolean right = canBeModifiedToInList(((OrExpression) ex).getRightExpression(), columnName);
            return left && right;
        }
        return false;
    }
}
