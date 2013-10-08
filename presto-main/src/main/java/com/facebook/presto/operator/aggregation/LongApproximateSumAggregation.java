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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.block.Block;
import com.facebook.presto.block.BlockBuilder;
import com.facebook.presto.block.BlockCursor;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleInfo.Type;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.facebook.presto.tuple.TupleInfo.SINGLE_VARBINARY;

public class LongApproximateSumAggregation
        implements FixedWidthAggregationFunction
{
    public static final LongApproximateSumAggregation LONG_APPROX_SUM = new LongApproximateSumAggregation();

    /**
     * Describes the tuple used by to calculate the approx sum.
     */
    static final TupleInfo APPROX_SUM_CONTEXT_INFO = new TupleInfo(
            Type.FIXED_INT_64,  // n
            Type.FIXED_INT_64,  // sum
            Type.DOUBLE);       // m2

    @Override
    public int getFixedSize()
    {
        return APPROX_SUM_CONTEXT_INFO.getFixedSize();
    }

    @Override
    public TupleInfo getFinalTupleInfo()
    {
        //TODO: This must be fixed when we have primitive error types implemented
        return SINGLE_VARBINARY;
    }

    @Override
    public TupleInfo getIntermediateTupleInfo()
    {
        return SINGLE_VARBINARY;
    }

    @Override
    public void initialize(Slice valueSlice, int valueOffset)
    {
        // mark value null

        APPROX_SUM_CONTEXT_INFO.setNull(valueSlice, valueOffset, 0);

        APPROX_SUM_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 1);
        //APPROX_SUM_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 1, 0);

        APPROX_SUM_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 2);
        APPROX_SUM_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, 0);

    }

    @Override
    public void addInput(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        boolean hasValue = !APPROX_SUM_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);

        if (cursor.isNull(field)) {
            return;
        }

        long count = hasValue ? APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        long sum = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 1);
        double m2 = APPROX_SUM_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);
        double mean = hasValue? ((double) sum) / count : 0;

        count++;
        long x = cursor.getLong(field);
        double delta = x - mean;
        sum += x;
        mean += (delta / count);
        m2 += (delta * (x - mean));

        if (!hasValue) {
            APPROX_SUM_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
        }

        APPROX_SUM_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, count);
        APPROX_SUM_CONTEXT_INFO.setLong(valueSlice, valueOffset, 1, sum);
        APPROX_SUM_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, m2);

    }

    @Override
    public void addInput(int positionCount, Block block, int field, Slice valueSlice, int valueOffset)
    {
        boolean hasValue = !APPROX_SUM_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);
        long count = hasValue ? APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0) : 0;
        long sum = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 1);
        double m2 = APPROX_SUM_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);
        double mean = hasValue? ((double) sum) / count : 0;

        BlockCursor cursor = block.cursor();

        while (cursor.advanceNextPosition()) {
            if (cursor.isNull(field)) {
                continue;
            }

            // There is now at least one value present.
            hasValue = true;

            count++;
            double x = cursor.getLong(field);
            double delta = x - mean;
            sum += x;
            mean += (delta / count);
            m2 += (delta * (x - mean));
        }

        if (hasValue) {
            APPROX_SUM_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
            APPROX_SUM_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, count);
            APPROX_SUM_CONTEXT_INFO.setLong(valueSlice, valueOffset, 1, sum);
            APPROX_SUM_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, m2);
        }

    }

    @Override
    public void addIntermediate(BlockCursor cursor, int field, Slice valueSlice, int valueOffset)
    {
        if (cursor.isNull(field)) {
            return;
        }

        Slice otherVariance = cursor.getSlice(field);
        long otherCount = APPROX_SUM_CONTEXT_INFO.getLong(otherVariance, 0);
        long otherSum = APPROX_SUM_CONTEXT_INFO.getLong(otherVariance, 1);
        double otherM2 = APPROX_SUM_CONTEXT_INFO.getDouble(otherVariance, 2);
        double otherMean = (otherCount != 0)? ((double) otherSum) / otherCount : 0;

        long totalCount;
        long totalSum;
        double totalM2;
        //double totalMean;

        if (APPROX_SUM_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0)) {
            totalCount = otherCount;
            totalSum = otherSum;
            totalM2 = otherM2;
        }
        else {
            long count = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
            long sum = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 1);
            double m2 = APPROX_SUM_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);
            double mean = (count != 0)? ((double) sum) / count : 0;
            double delta = otherMean - mean;

            totalCount = count + otherCount;

            // Use numerically stable variant
            totalSum = sum + otherSum;
            //totalMean = ((count * mean) + (otherCount * otherMean)) / totalCount;
            totalM2 = m2 + otherM2 + ((delta * delta) * (count * otherCount)) / totalCount;
        }

        APPROX_SUM_CONTEXT_INFO.setNotNull(valueSlice, valueOffset, 0);
        APPROX_SUM_CONTEXT_INFO.setLong(valueSlice, valueOffset, 0, totalCount);
        APPROX_SUM_CONTEXT_INFO.setLong(valueSlice, valueOffset, 1, totalSum);
        APPROX_SUM_CONTEXT_INFO.setDouble(valueSlice, valueOffset, 2, totalM2);
    }

    @Override
    public void evaluateIntermediate(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        boolean isEmpty = APPROX_SUM_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0);
        if (isEmpty) {
            output.appendNull();
            return;
        }

        long count = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
        long sum = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 1);
        double m2 = APPROX_SUM_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);

        Slice intermediateValue = Slices.allocate(APPROX_SUM_CONTEXT_INFO.getFixedSize());
        APPROX_SUM_CONTEXT_INFO.setNotNull(intermediateValue, 0);
        APPROX_SUM_CONTEXT_INFO.setLong(intermediateValue, 0, count);
        APPROX_SUM_CONTEXT_INFO.setLong(intermediateValue, 1, sum);
        APPROX_SUM_CONTEXT_INFO.setDouble(intermediateValue, 2, m2);

        output.append(intermediateValue);
    }

    @Override
    public void evaluateFinal(Slice valueSlice, int valueOffset, BlockBuilder output)
    {
        if (!APPROX_SUM_CONTEXT_INFO.isNull(valueSlice, valueOffset, 0)) {

            long count = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 0);
            long sum = APPROX_SUM_CONTEXT_INFO.getLong(valueSlice, valueOffset, 1);
            double m2 = APPROX_SUM_CONTEXT_INFO.getDouble(valueSlice, valueOffset, 2);
            double variance = m2 / count;

            // The multiplier 2.575 corresponds to the z-score of 99% confidence interval
            // (http://upload.wikimedia.org/wikipedia/commons/b/bb/Normal_distribution_and_scales.gif)
            double zScore = 2.575;

            // Error bars at 99% confidence interval
            StringBuilder sb = new StringBuilder();
            sb.append(sum);
            sb.append(" +/- ");
            sb.append(zScore * Math.sqrt(variance / count));

            output.append(sb.toString());

        }
        else {
            output.appendNull();
        }
    }
}
