/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.planner.plan;

import com.facebook.presto.sql.planner.Symbol;
import com.facebook.presto.sql.tree.SampledRelation;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import javax.annotation.concurrent.Immutable;

import java.util.List;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

@Immutable
public class SampleNode
        extends PlanNode
{
    private final PlanNode source;
    private final double sampleRatio;
    private final Type sampleType;
    private final Symbol weightOutput;

    public enum Type
    {
        BERNOULLI,
        POISSONIZED,
        SYSTEM;

        public static Type fromType(SampledRelation.Type sampleType)
        {
            switch (sampleType) {
                case BERNOULLI:
                    return Type.BERNOULLI;
                case POISSONIZED:
                    return Type.POISSONIZED;
                case SYSTEM:
                    return Type.SYSTEM;
                default:
                    throw new UnsupportedOperationException("Unsupported sample type: " + sampleType);
            }
        }
    }

    @JsonCreator
    public SampleNode(
            @JsonProperty("id") PlanNodeId id,
            @JsonProperty("source") PlanNode source,
            @JsonProperty("sampleRatio") double sampleRatio,
            @JsonProperty("sampleType") Type sampleType,
            @JsonProperty("weightOutput") Symbol weightOutput)
    {
        super(id);

        checkArgument(sampleRatio >= 0.0, "sample ratio must be greater than or equal to 0");
        checkArgument((sampleRatio <= 1.0) || (sampleType == Type.POISSONIZED) , "sample ratio must be less than or equal to 1");

        this.weightOutput = checkNotNull(weightOutput, "weightOutput is null");
        this.sampleType = checkNotNull(sampleType, "sample type is null");
        this.source = checkNotNull(source, "source is null");
        this.sampleRatio = checkNotNull(sampleRatio, "sample ratio is null");
    }

    @Override
    public List<PlanNode> getSources()
    {
        return ImmutableList.of(source);
    }

    @JsonProperty
    public PlanNode getSource()
    {
        return source;
    }

    @JsonProperty
    public double getSampleRatio()
    {
        return sampleRatio;
    }

    @JsonProperty
    public Type getSampleType()
    {
        return sampleType;
    }

    @JsonProperty
    public Symbol getWeightOutput()
    {
        return weightOutput;
    }

    @Override
    public List<Symbol> getOutputSymbols()
    {
        return ImmutableList.<Symbol>builder()
                .addAll(source.getOutputSymbols())
                .add(weightOutput)
                .build();
    }

    @Override
    public <C, R> R accept(PlanVisitor<C, R> visitor, C context)
    {
        return visitor.visitSample(this, context);
    }
}
