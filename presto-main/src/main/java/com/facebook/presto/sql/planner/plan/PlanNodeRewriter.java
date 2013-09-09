package com.facebook.presto.sql.planner.plan;

public class PlanNodeRewriter<C>
{
    public PlanNode rewriteNode(PlanNode node, C context, PlanRewriter<C> planRewriter)
    {
        return null;
    }

    public PlanNode rewriteLimit(LimitNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteExchange(ExchangeNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteTopN(TopNNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteTableScan(TableScanNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteProject(ProjectNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteFilter(FilterNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteSample(SampleNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteJoin(JoinNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteSemiJoin(SemiJoinNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteAggregation(AggregationNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteWindow(WindowNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteOutput(OutputNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteSort(SortNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteTableWriter(TableWriterNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }

    public PlanNode rewriteUnion(UnionNode node, C context, PlanRewriter<C> planRewriter)
    {
        return rewriteNode(node, context, planRewriter);
    }
}
