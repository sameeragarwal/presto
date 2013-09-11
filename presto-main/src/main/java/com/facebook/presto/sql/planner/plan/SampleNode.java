package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.Expression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;
import java.util.List;

@Immutable
public class SampleNode
        extends PlanNode {
    private final PlanNode source;
    private final Expression samplePercentage;

    @JsonCreator
    public SampleNode(@JsonProperty("id") PlanNodeId id, @JsonProperty("source") PlanNode source, @JsonProperty("samplePercentage") Expression samplePercentage)
    {
        super(id);

        Preconditions.checkNotNull(source, "source is null");
        this.source = source;
        this.samplePercentage = samplePercentage;
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty("source")
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty("samplePercentage")
    public Expression getSamplePercentage()
    {
        return samplePercentage;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return source.getOutputSymbols();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSample(this, context);
    }
}