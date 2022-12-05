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
                Expression join = null;
                if (sql.getWhere() == null) {
                    for (Expression joinExp : sql.getJoins().get(0).getOnExpressions()) {
                        join = joinExp;
                    }
                } else {
                    join = (EqualsTo) getJoinPredicate(sql.getWhere());
                }
                List<Expression> predicateList = new ArrayList<Expression>();
                getPredicateList(sql.getWhere(), predicateList);

                if (predicateList.size() <= 1) {
                    // No Predicates
                    twoTableWithIndexNoPredicate(sql, indexes, planTable, predicateTable, join);
                } else {
                    // Has Predicates
                    twoTableWithIndexWithPredicate(sql, indexes, planTable, predicateTable, join);
                }
            }
        }
    }

    private static void twoTableWithIndexWithPredicate(PlainSelect sql, List<DbIndex> indexes, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable, Expression joinExp) {
        String t1 = ((Column) (((EqualsTo) joinExp).getLeftExpression())).getTable().getName();
        Integer t1JoinCol = Integer.parseInt(((Column) (((EqualsTo) joinExp).getLeftExpression())).getColumnName());
        String t2 = ((Column) (((EqualsTo) joinExp).getRightExpression())).getTable().getName();
        Integer t2JoinCol = Integer.parseInt(((Column) (((EqualsTo) joinExp).getRightExpression())).getColumnName());
        DbTable table1Stats = new DbTable(t1, true);
        DbTable table2Stats = new DbTable(t2, true);

        List<DbIndex> t1Indexes = new ArrayList<DbIndex>();
        List<DbIndex> t2Indexes = new ArrayList<DbIndex>();
        for (DbIndex idx : indexes) {
            if (idx.getIndexName().contains(t1)) {
                t1Indexes.add(idx);
            } else if (idx.getIndexName().contains(t2)) {
                t2Indexes.add(idx);
            }
        }
        planTable.AccessType = 'I';
        planTable.Table1Card = table1Stats.getNumRows();
        planTable.Table2Card = table2Stats.getNumRows();

        List<Expression> t1PredicateList = new ArrayList<>();
        List<Expression> t2PredicateList = new ArrayList<>();
        getPredicateList(sql.getWhere(), t1PredicateList, t1);
        getPredicateList(sql.getWhere(), t2PredicateList, t2);
        DbIndex bestIndex = null;
        DbIndex t1BestIdx = evaluateBestIndex(filterIndexesOnJoinColumn(t1Indexes, t1JoinCol), t1PredicateList, planTable, table1Stats);
        int t1MatchCols = planTable.MatchCols;
        DbIndex t2BestIdx = evaluateBestIndex(filterIndexesOnJoinColumn(t2Indexes, t2JoinCol), t2PredicateList, planTable, table2Stats);
        if (t1BestIdx == null && t2BestIdx == null) {
            planTable.MatchCols = 0;
            planTable.AccessName = "";
            planTable.AccessType = 'R';
            if (table1Stats.getNumRows() > table2Stats.getNumRows()) {
                planTable.LeadingTable = t1;
                planTable.InnerTable = t2;
            } else {
                planTable.LeadingTable = t2;
                planTable.InnerTable = t1;
            }
            planTable.IndexOnly = 'N';
            planTable.Prefetch = "S";
            setSortCompositeOrderBy(sql, null, planTable);
        } else if (t1BestIdx == null) {
            planTable.AccessName = t2BestIdx.getIndexName();
            planTable.AccessType = 'I';
            planTable.LeadingTable = t1;
            planTable.InnerTable = t2;

            planTable.IndexOnly = indexOnly(sql, t2BestIdx);
            planTable.Prefetch = planTable.IndexOnly == 'N' ? "S" : "";
            setSortCompositeOrderBy(sql, null, planTable);
            bestIndex = t2BestIdx;
        } else if (t2BestIdx == null) {
            planTable.AccessName = t1BestIdx.getIndexName();
            planTable.AccessType = 'I';
            planTable.LeadingTable = t2;
            planTable.InnerTable = t1;

            planTable.IndexOnly = indexOnly(sql, t1BestIdx);
            planTable.Prefetch = planTable.IndexOnly == 'N' ? "S" : "";
            setSortCompositeOrderBy(sql, null, planTable);
            bestIndex = t1BestIdx;
        } else {
            if (planTable.MatchCols < t1MatchCols) {
                planTable.MatchCols = t1MatchCols;
                planTable.AccessName = t1BestIdx.getIndexName();
                planTable.AccessType = 'I';
                planTable.LeadingTable = t2;
                planTable.InnerTable = t1;
                planTable.IndexOnly = indexOnly(sql, t1BestIdx);
                planTable.Prefetch = planTable.IndexOnly == 'N' ? "S" : "";
                setSortCompositeOrderBy(sql, t2BestIdx, planTable);
                bestIndex = t1BestIdx;
            } else if (planTable.MatchCols > t1MatchCols) {
                planTable.AccessName = t2BestIdx.getIndexName();
                planTable.AccessType = 'I';
                planTable.LeadingTable = t1;
                planTable.InnerTable = t2;
                planTable.IndexOnly = indexOnly(sql, t2BestIdx);
                planTable.Prefetch = planTable.IndexOnly == 'N' ? "S" : "";
                setSortCompositeOrderBy(sql, t1BestIdx, planTable);
                bestIndex = t2BestIdx;
            } else {
                // Same number of matching columns
                Double t1ff = getCombinedLocalPredicateFF(t1, t1PredicateList, table1Stats);
                Double t2ff = getCombinedLocalPredicateFF(t2, t2PredicateList, table2Stats);
                if (t1ff < t2ff) {
                    // T1 is small table - OUTER
                    // T2 is big table - INNER
                    planTable.AccessName = t2BestIdx.getIndexName();
                    planTable.AccessType = 'I';
                    planTable.LeadingTable = t1;
                    planTable.InnerTable = t2;
                    planTable.IndexOnly = indexOnly(sql, t2BestIdx);
                    planTable.Prefetch = planTable.IndexOnly == 'N' ? "S" : "";
                    setSortCompositeOrderBy(sql, t1BestIdx, planTable);
                    bestIndex = t2BestIdx;
                } else {
                    // T2 is small table - OUTER
                    // T1 is big table - INNER
                    planTable.AccessName = t1BestIdx.getIndexName();
                    planTable.AccessType = 'I';
                    planTable.LeadingTable = t2;
                    planTable.InnerTable = t1;
                    planTable.IndexOnly = indexOnly(sql, t1BestIdx);
                    planTable.Prefetch = planTable.IndexOnly == 'N' ? "S" : "";
                    setSortCompositeOrderBy(sql, t2BestIdx, planTable);
                    bestIndex = t1BestIdx;
                }
            }
        }
        DbQueryPlanTable.printTable(planTable);
        if (bestIndex != null) {
            if (bestIndex.getIndexName().contains(t1)) {
                sequence = 1;
                predNo = 1;
                setOuterPredicateTable(t2PredicateList, predicateTable, table2Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t2);
                sequence = predicateTable.rows.size() + 1;
                DbQueryPredicateTableRow joinRow = new DbQueryPredicateTableRow(joinExp, predNo++, table1Stats, table2Stats);
                joinRow.Sequence = sequence++;
                predicateTable.rows.add(joinRow);
                setPredicateTable(bestIndex, t1PredicateList, predicateTable, table1Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t1);
            } else {
                sequence = 1;
                predNo = 1;
                setOuterPredicateTable(t1PredicateList, predicateTable, table1Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t1);
                sequence = predicateTable.rows.size() + 1;
                DbQueryPredicateTableRow joinRow = new DbQueryPredicateTableRow(joinExp, predNo++, table1Stats, table2Stats);
                joinRow.Sequence = sequence++;
                predicateTable.rows.add(joinRow);
                setPredicateTable(bestIndex, t2PredicateList, predicateTable, table2Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t2);
            }
        } else {
            if (planTable.LeadingTable.equals(t1)) {
                // If leading table is T1 Evaluate T1 predicates first
                sequence = 1;
                predNo = 1;
                setOuterPredicateTable(t1PredicateList, predicateTable, table1Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t1);
                sequence = predicateTable.rows.size() + 1;
                DbQueryPredicateTableRow joinRow = new DbQueryPredicateTableRow(joinExp, predNo++, table1Stats, table2Stats);
                joinRow.Sequence = sequence++;
                predicateTable.rows.add(joinRow);
                setOuterPredicateTable(t2PredicateList, predicateTable, table2Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t2);
            } else {
                sequence = 1;
                predNo = 1;
                setOuterPredicateTable(t2PredicateList, predicateTable, table2Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t2);
                sequence = predicateTable.rows.size() + 1;
                DbQueryPredicateTableRow joinRow = new DbQueryPredicateTableRow(joinExp, predNo++, table1Stats, table2Stats);
                joinRow.Sequence = sequence++;
                predicateTable.rows.add(joinRow);
                setOuterPredicateTable(t1PredicateList, predicateTable, table1Stats);
                predicateTable.setPredicateSequenceFF1ByTable(t1);
            }
        }
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static void setOuterPredicateTable(List<Expression> predicateList, DbQueryPredicateTable predicateTable, DbTable tableStats) {
        for (Expression ex : predicateList) {
            predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, tableStats));
        }
    }

    private static char indexOnly(PlainSelect sql, DbIndex bestIdx) {
        int maxMatch = 0;
        List<String> columnNames = new ArrayList<String>();
        if (bestIdx != null) {
            for (SelectItem si : sql.getSelectItems()) {
                Column column = (Column) ((SelectExpressionItem) si).getExpression();
                columnNames.add(column.getColumnName());
            }
            for (Integer col : bestIdx.getIndexColumns()) {
                for (String name : columnNames) {
                    if (name.equals(col.toString())) {
                        maxMatch++;
                    }
                }
            }
        }
        return (maxMatch == columnNames.size() ? 'Y' : 'N');
    }

    private static Double getCombinedLocalPredicateFF(String tableName, List<Expression> predicateList, DbTable tableStats) {
        Double ff = 1.0;
        for (Expression ex : predicateList) {
            if (ex instanceof EqualsTo) {
                EqualsTo eq = (EqualsTo) ex;
                Column col = (Column) eq.getLeftExpression();
                ff *= 1.0 / tableStats.getColumnCardinality(Integer.parseInt(col.getColumnName()));
            } else if (ex instanceof GreaterThan) {
                GreaterThan gt = (GreaterThan) ex;
                Column col = (Column) gt.getLeftExpression();

                Integer value = Integer.parseInt(((LongValue) gt.getRightExpression()).getStringValue());
                Integer columnNumber = Integer.parseInt(col.getColumnName());
                Double highKey = tableStats.getHighKey(columnNumber).doubleValue();
                Double lowKey = tableStats.getLowKey(columnNumber).doubleValue();
                if (!(value.doubleValue() < lowKey || value.doubleValue() > highKey)) {
                    ff *= (highKey - value.doubleValue()) / (highKey - lowKey + 1.0);
                }
            } else if (ex instanceof MinorThan) {
                MinorThan mt = (MinorThan) ex;
                Column col = (Column) mt.getLeftExpression();

                Integer value = Integer.parseInt(((LongValue) mt.getRightExpression()).getStringValue());
                Integer columnNumber = Integer.parseInt(col.getColumnName());
                Double highKey = tableStats.getHighKey(columnNumber).doubleValue();
                Double lowKey = tableStats.getLowKey(columnNumber).doubleValue();
                if (!(value.doubleValue() < lowKey || value.doubleValue() > highKey)) {
                    ff *= (value.doubleValue() - lowKey) / (highKey - lowKey + 1.0);
                }
            }
        }
        return ff;
    }

    private static List<DbIndex> filterIndexesOnJoinColumn(List<DbIndex> indexes, Integer joinCol) {
        List<DbIndex> filtered = new ArrayList<DbIndex>();
        for (DbIndex idx : indexes) {
            if (idx.getIndexColumns().get(0).equals(joinCol)) {
                filtered.add(idx);
            }
        }
        return filtered;
    }

    private static void twoTableWithIndexNoPredicate(PlainSelect sql, List<DbIndex> indexes, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable, Expression joinExp) {
        String t1 = ((Column) (((EqualsTo) joinExp).getLeftExpression())).getTable().getName();
        String t2 = ((Column) (((EqualsTo) joinExp).getRightExpression())).getTable().getName();
        Integer t1JoinColumn = Integer.parseInt(((Column) (((EqualsTo) joinExp).getLeftExpression())).getColumnName());
        Integer t2JoinColumn = Integer.parseInt(((Column) (((EqualsTo) joinExp).getRightExpression())).getColumnName());
        DbTable table1Stats = new DbTable(t1, true);
        DbTable table2Stats = new DbTable(t2, true);

        List<DbIndex> t1Indexes = new ArrayList<DbIndex>();
        List<DbIndex> t2Indexes = new ArrayList<DbIndex>();
        for (DbIndex idx : indexes) {
            if (idx.getIndexName().contains(t1)) {
                t1Indexes.add(idx);
            } else if (idx.getIndexName().contains(t2)) {
                t2Indexes.add(idx);
            }
        }
        planTable.Table1Card = table1Stats.getNumRows();
        planTable.Table2Card = table2Stats.getNumRows();
        Integer t1MatchCols = 0, t2MatchCols = 0;

        Set<String> t1ColumnNames = new HashSet<String>();
        Set<String> t2ColumnNames = new HashSet<String>();
        for (SelectItem si : sql.getSelectItems()) {
            Column column = (Column) ((SelectExpressionItem) si).getExpression();
            if (column.getTable().getName().equals(t1)) {
                t1ColumnNames.add(column.getColumnName());
            } else {
                t2ColumnNames.add(column.getColumnName());
            }
        }
        int curMatch = 0;
        int t1MaxMatch = 0;
        int t2MaxMatch = 0;
        DbIndex t1BestIdx = null;
        DbIndex t2BestIdx = null;
        List<DbIndex> filtered = filterIndexesOnJoinColumn(indexes, t1JoinColumn);
        filtered.addAll(filterIndexesOnJoinColumn(t1Indexes, t2JoinColumn));
        for (DbIndex idx : filtered) {
            if (idx.getIndexName().contains(t1)) {
                curMatch = 0;
                for (Integer col : idx.getIndexColumns()) {
                    if (t1ColumnNames.contains(col.toString())) {
                        curMatch++;
                    }
                }
                if ((idx.getIndexName().contains(t1) && curMatch > t1MaxMatch && idx.getIndexColumns().contains(t1JoinColumn)) || (t1BestIdx == null && idx.getIndexColumns().contains(t1JoinColumn))) {
                    t1MaxMatch = curMatch;
                    t1BestIdx = idx;
                }
            } else {
                curMatch = 0;
                for (Integer col : idx.getIndexColumns()) {
                    if (t2ColumnNames.contains(col.toString())) {
                        curMatch++;
                    }
                }
                if ((idx.getIndexName().contains(t2) && curMatch > t2MaxMatch && idx.getIndexColumns().contains(t2JoinColumn)) || (t1BestIdx == null && idx.getIndexColumns().contains(t1JoinColumn))) {
                    t2MaxMatch = curMatch;
                    t2BestIdx = idx;
                }
            }
        }

        if (t1MaxMatch > t2MaxMatch) {
            planTable.LeadingTable = t2;
            planTable.MatchCols = t1MaxMatch;
            planTable.IndexOnly = t1BestIdx != null ? (t1MaxMatch == t1ColumnNames.size() ? 'Y' : 'N') : 'N';
            planTable.AccessName = t1BestIdx != null ? t1BestIdx.getIndexName() : "";
        } else if (t1MaxMatch < t2MaxMatch) {
            planTable.LeadingTable = t1;
            planTable.MatchCols = t2MaxMatch;
            planTable.IndexOnly = t2BestIdx != null ? (t2MaxMatch == t2ColumnNames.size() ? 'Y' : 'N') : 'N';
            planTable.AccessName = t2BestIdx != null ? (t2BestIdx.getIndexName()) : "";
        } else {
            if (table1Stats.getNumRows() < table2Stats.getNumRows()) {
                planTable.LeadingTable = t2;
                planTable.MatchCols = t1MaxMatch;
                planTable.IndexOnly = t1BestIdx != null ? (t1MaxMatch == t1ColumnNames.size() ? 'Y' : 'N') : 'N';
                planTable.AccessName = t1BestIdx != null ? t1BestIdx.getIndexName() : "";
            } else {
                planTable.LeadingTable = t1;
                planTable.MatchCols = t2MaxMatch;
                planTable.IndexOnly = t2BestIdx != null ? (t2MaxMatch == t2ColumnNames.size() ? 'Y' : 'N') : 'N';
                planTable.AccessName = t2BestIdx != null ? t2BestIdx.getIndexName() : "";
            }
        }

        planTable.AccessType = t1BestIdx == null && t2BestIdx == null ? 'R' : 'I';
        if (planTable.IndexOnly == 'Y') {
            planTable.Prefetch = "";
        } else {
            planTable.Prefetch = "S";
        }
        if (sql.getOrderByElements() != null) {
            planTable.SortC_OrderBy = 'Y';
        } else {
            planTable.SortC_OrderBy = 'N';
        }
        DbQueryPlanTable.printTable(planTable);
        DbQueryPredicateTableRow row = new DbQueryPredicateTableRow(joinExp, 1, table1Stats, table2Stats);
        row.Sequence = 1;
        predicateTable.rows.add(row);
        DbQueryPredicateTable.printTable(predicateTable);
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
        planTable.IndexOnly = bestIdx != null && columnNames.size() <= bestIdx.getNumIndexColumns() && maxMatch == columnNames.size() ? 'Y' : 'N';
        planTable.Prefetch = bestIdx != null && maxMatch != bestIdx.getNumIndexColumns() ? "S" : "";

        setSortCompositeOrderBy(sql, bestIdx, planTable);
        planTable.Table1Card = tableStats.getNumRows();
        DbQueryPlanTable.printTable(planTable);
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static void singleTableWithIndexWithPredicate(PlainSelect sql, List<DbIndex> indexes, DbQueryPlanTable planTable, DbQueryPredicateTable predicateTable) {
        String tableName = ((Table) sql.getFromItem()).getName();
        DbTable tableStats = new DbTable(tableName, true);
        if (sql.getWhere() instanceof AndExpression || sql.getWhere() instanceof EqualsTo || sql.getWhere() instanceof GreaterThan || sql.getWhere() instanceof MinorThan) {
            List<Expression> predicateList = new ArrayList<>();
            getPredicateList(sql.getWhere(), predicateList);
            DbIndex bestIdx = evaluateBestIndex(indexes, predicateList, planTable, tableStats);

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

            setSortCompositeOrderBy(sql, bestIdx, planTable);
            planTable.Table1Card = tableStats.getNumRows();

            DbQueryPlanTable.printTable(planTable);
            predNo = 1;
            sequence = 1;
            setPredicateTable(bestIdx, predicateList, predicateTable, tableStats);
            predicateTable.setPredicateSequenceFF1();
            DbQueryPredicateTable.printTable(predicateTable);

        } else if (sql.getWhere() instanceof OrExpression) {
            // TODO
            List<Expression> predicateList = new ArrayList<>();
            getPredicateList(sql.getWhere(), predicateList);
            DbIndex bestIdx = evaluateBestIndex(indexes, predicateList, planTable, tableStats);

            //planTable.AccessType = bestIdx != null ? 'I' : 'R';
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

            setSortCompositeOrderBy(sql, bestIdx, planTable);
            planTable.Table1Card = tableStats.getNumRows();

            OrExpression or = (OrExpression) sql.getWhere();
            predNo = 1;
            checkInListModification(sql.getWhere(), predicateTable, tableStats);
            for (DbQueryPredicateTableRow row : predicateTable.rows) {
                if (row.getPredicate() instanceof OrExpression) {
                    planTable.AccessType = 'N';
                    break;
                } else if (bestIdx != null) {
                    planTable.AccessType = 'I';
                } else {
                    planTable.AccessType = 'R';
                }
            }
            predicateTable.setPredicateSequenceFF1();
            DbQueryPlanTable.printTable(planTable);
            DbQueryPredicateTable.printTable(predicateTable);
        } else if (sql.getWhere() instanceof InExpression) {
            InExpression in = (InExpression) sql.getWhere();
            Column inColumn = (Column) in.getLeftExpression();
            DbIndex bestIdx = null;
            planTable.AccessType = 'N';
            planTable.Table1Card = tableStats.getNumRows();

            for (DbIndex idx : indexes) {
                if (idx.getIndexColumns().get(0).toString().equals(inColumn.getColumnName())) {
                    bestIdx = idx;
                }
            }
            planTable.AccessName = bestIdx!=null? bestIdx.getIndexName(): "";
            planTable.IndexOnly = bestIdx!=null? 'Y': 'N';
            planTable.MatchCols = bestIdx!=null? 1: 0;
            planTable.Prefetch = planTable.IndexOnly == 'Y'? "": "S";
            
            setSortCompositeOrderBy(sql, bestIdx, planTable);

            DbQueryPlanTable.printTable(planTable);
            predicateTable.rows.add(new DbQueryPredicateTableRow(in, 1, tableStats));
            DbQueryPredicateTable.printTable(predicateTable);
        }
    }

    private static void checkInListModification(Expression ex, DbQueryPredicateTable predicateTable, DbTable tableStats) {
        if (ex instanceof EqualsTo) {
            EqualsTo eq = (EqualsTo) ex;
            predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, tableStats));
        } else if (ex instanceof OrExpression) {
            OrExpression or = (OrExpression) ex;
            if ((or.getRightExpression() instanceof EqualsTo) && canBeModifiedToInList(or, ((Column) ((EqualsTo) or.getRightExpression()).getLeftExpression()).getColumnName())) {
                predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, tableStats));
            } else {
                checkInListModification(or.getLeftExpression(), predicateTable, tableStats);
                checkInListModification(or.getRightExpression(), predicateTable, tableStats);
            }
        }
    }

    private static void setSortCompositeOrderBy(PlainSelect sql, DbIndex bestIdx, DbQueryPlanTable planTable) {
        if (sql.getOrderByElements() != null) {
            if (bestIdx != null) {
                int j = 1;
                boolean flag = false;
                for (OrderByElement obe : sql.getOrderByElements()) {
                    Column column = ((Column) obe.getExpression());
                    Integer oCol = Integer.valueOf(column.getColumnName());
                    if (!((j - 1) < bestIdx.getNumIndexColumns() && bestIdx.getIndexName().contains(column.getTable().getName()) && bestIdx.getIndexColumn(j) == oCol && bestIdx.getIndexColumnOrder(j) == (obe.isAsc() ? 'A' : 'D'))) {
                        planTable.SortC_OrderBy = 'Y';
                        flag = true;
                        break;
                    }
                    j++;
                }
                if (!flag) {
                    planTable.SortC_OrderBy = 'N';
                }
            } else {
                planTable.SortC_OrderBy = 'Y';
            }
        } else {
            planTable.SortC_OrderBy = 'N';
        }
    }

    private static void setPredicateTable(DbIndex idx, List<Expression> predicateList, DbQueryPredicateTable predicateTable, DbTable tableStats) {

        Expression cur = null;
        for (Integer column : idx.getIndexColumns()) {
            for (Expression ex : predicateList) {
                if (ex instanceof EqualsTo) {
                    EqualsTo eq = (EqualsTo) ex;
                    if (((Column) eq.getLeftExpression()).getColumnName().equals(column.toString())) {
                        DbQueryPredicateTableRow row = new DbQueryPredicateTableRow(ex, predNo++, tableStats);
                        row.Sequence = sequence++;
                        predicateTable.rows.add(row);
                        cur = ex;
                        break;
                    }
                } else if (ex instanceof GreaterThan) {
                    Column c = (Column) ((GreaterThan) ex).getLeftExpression();
                    if (c.getColumnName().equals(column.toString())) {
                        DbQueryPredicateTableRow row = new DbQueryPredicateTableRow(ex, predNo++, tableStats);
                        row.Sequence = sequence++;
                        predicateTable.rows.add(row);
                        cur = ex;
                        break;
                    }
                } else if (ex instanceof MinorThan) {
                    Column c = (Column) ((MinorThan) ex).getLeftExpression();
                    if (c.getColumnName().equals(column.toString())) {
                        DbQueryPredicateTableRow row = new DbQueryPredicateTableRow(ex, predNo++, tableStats);
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
            predicateTable.rows.add(new DbQueryPredicateTableRow(ex, predNo++, tableStats));
        }
    }

    private static DbIndex evaluateBestIndex(List<DbIndex> indexes, List<Expression> predicateList, DbQueryPlanTable planTable, DbTable tableStats) {
        Map<String, HashMap<String, Integer>> predicateMap = new HashMap<String, HashMap<String, Integer>>();

        for (DbIndex idx : indexes) {
            List<Integer> predicateEvaluation = new ArrayList<>(); // 0=EQUALS, 1=GREATER_THAN, -1=MINOR_THAN
            predicateEvaluation.add(0);
            HashMap<String, Integer> idxMap = new HashMap<String, Integer>();
            char phase = 'M';
            for (Integer column : idx.getIndexColumns()) {
                for (Expression ex : predicateList) {
                    if (ex instanceof EqualsTo) {
                        EqualsTo eq = (EqualsTo) ex;
                        if (((Column) eq.getLeftExpression()).getColumnName().equals(column.toString()) && phase == 'M') {
                            if (predicateEvaluation.get(predicateEvaluation.size() - 1) == 0) {
                                idxMap.put("MATCHING", idxMap.getOrDefault("MATCHING", 0) + 1);
                                predicateEvaluation.add(0);
                                break;
                            } else if (predicateEvaluation.get(predicateEvaluation.size() - 1) != 0) { // Screening
                                phase = 'S';
                                idxMap.put("SCREENING", idxMap.getOrDefault("SCREENING", 0) + 1);
                                predicateEvaluation.add(0);
                                break;
                            }
                        } else if (((Column) eq.getLeftExpression()).getColumnName().equals(column.toString()) && phase == 'S') {
                            idxMap.put("SCREENING", idxMap.getOrDefault("SCREENING", 0) + 1);
                            predicateEvaluation.add(0);
                            break;
                        }
                    } else if (ex instanceof GreaterThan) {
                        Column c = (Column) ((GreaterThan) ex).getLeftExpression();
                        if (c.getColumnName().equals(column.toString()) && phase == 'M') {
                            if (predicateEvaluation.get(predicateEvaluation.size() - 1) == 0) {
                                idxMap.put("MATCHING", idxMap.getOrDefault("MATCHING", 0) + 1);
                                predicateEvaluation.add(1);
                                break;
                            } else if (predicateEvaluation.get(predicateEvaluation.size() - 1) != 0) { // Screening
                                phase = 'S';
                                idxMap.put("SCREENING", idxMap.getOrDefault("SCREENING", 0) + 1);
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
                // DONE:TODO break tie with FF

                if (bestIndex != null) {
                    // FF for current table
                    Double curIndexFF = getCombinedIndexPredicateFF(idx, predicateList, tableStats);
                    Double bestIndexFF = getCombinedIndexPredicateFF(bestIndex, predicateList, tableStats);
                    if (curIndexFF < bestIndexFF) {
                        bestIndex = idx;
                    }
                } else {
                    bestIndex = idx;
                }
            }
        }
        planTable.MatchCols = bestIndex != null ? predicateMap.get(bestIndex.getIndexName()).getOrDefault("MATCHING", 0) : 0;
        return bestIndex;
    }

    private static Double getCombinedIndexPredicateFF(DbIndex idx, List<Expression> predicateList, DbTable tableStats) {
        Double curFF = 1.0;
        for (Integer column : idx.getIndexColumns()) {
            for (Expression ex : predicateList) {
                if (ex instanceof EqualsTo) {
                    if (((Column) ((EqualsTo) ex).getLeftExpression()).getColumnName().equals(column.toString())) {
                        curFF *= tableStats.getColumnCardinality(column);
                    }
                } else if (ex instanceof GreaterThan) {
                    GreaterThan gt = (GreaterThan) ex;
                    if (((Column) gt.getLeftExpression()).getColumnName().equals(column.toString())) {
                        Integer value = Integer.parseInt(((LongValue) gt.getRightExpression()).getStringValue());
                        Integer columnNumber = column;
                        Double highKey = tableStats.getHighKey(columnNumber).doubleValue();
                        Double lowKey = tableStats.getLowKey(columnNumber).doubleValue();
                        if (!(value.doubleValue() < lowKey || value.doubleValue() > highKey)) {
                            curFF *= (highKey - value.doubleValue()) / (highKey - lowKey + 1.0);
                        }
                    }
                } else if (ex instanceof MinorThan) {
                    MinorThan mt = (MinorThan) ex;
                    if (((Column) mt.getLeftExpression()).getColumnName().equals(column.toString())) {
                        Integer value = Integer.parseInt(((LongValue) mt.getRightExpression()).getStringValue());
                        Integer columnNumber = column;
                        Double highKey = tableStats.getHighKey(columnNumber).doubleValue();
                        Double lowKey = tableStats.getLowKey(columnNumber).doubleValue();
                        if (!(value.doubleValue() < lowKey || value.doubleValue() > highKey)) {
                            curFF *= (value.doubleValue() - lowKey) / (highKey - lowKey + 1.0);
                        }
                    }
                }
            }
        }
        return curFF;
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

    private static void getPredicateList(Expression ex, List<Expression> result, String tableName) {
        if (ex instanceof EqualsTo && !(((EqualsTo) ex).getRightExpression() instanceof Column) && ((Column) ((EqualsTo) ex).getLeftExpression()).getTable().getName().equals(tableName)) {
            result.add((EqualsTo) ex);
        } else if (ex instanceof GreaterThan && ((Column) ((GreaterThan) ex).getLeftExpression()).getTable().getName().equals(tableName)) {
            result.add((GreaterThan) ex);
        } else if (ex instanceof MinorThan && ((Column) ((MinorThan) ex).getLeftExpression()).getTable().getName().equals(tableName)) {
            result.add((MinorThan) ex);
        } else if (ex instanceof AndExpression) {
            getPredicateList(((AndExpression) ex).getLeftExpression(), result, tableName);
            getPredicateList(((AndExpression) ex).getRightExpression(), result, tableName);
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

        // CANCELLED:TODO: process OR predicates on same table that are in alternate order. SELECT T2.1 FROM T2 WHERE T2.1=1 OR T2.2=2 OR T2.1=3 OR T2.2=4
        getPredicates(sql, tableStats, null, predicateTable);
        predicateTable.setPredicateSequenceFF1();
        DbQueryPredicateTable.printTable(predicateTable);
    }

    private static void getPredicates(PlainSelect sql, DbTable table1Stats, DbTable table2Stats, DbQueryPredicateTable predicateTable) {
        predNo = 1;
        getPredicatesRecursive(sql.getWhere(), table1Stats, table2Stats, predicateTable);
    }

    private static Integer predNo = 1;
    private static Integer sequence = 1;

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
            } else if (!((Column) eq.getLeftExpression()).getColumnName().equals(columnName)) {
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
