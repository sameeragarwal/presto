package com.facebook.presto.sql.planner;

import com.facebook.presto.execution.TaskMemoryManager;
import com.facebook.presto.metadata.FunctionHandle;
import com.facebook.presto.metadata.LocalStorageManager;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.operator.AggregationFunctionDefinition;
import com.facebook.presto.operator.AggregationOperator;
import com.facebook.presto.operator.ExchangeClient;
import com.facebook.presto.operator.ExchangeOperator;
import com.facebook.presto.operator.FilterAndProjectOperator;
import com.facebook.presto.operator.FilterFunction;
import com.facebook.presto.operator.FilterFunctions;
import com.facebook.presto.operator.HashAggregationOperator;
import com.facebook.presto.operator.HashJoinOperator;
import com.facebook.presto.operator.HashSemiJoinOperator;
import com.facebook.presto.operator.InMemoryOrderByOperator;
import com.facebook.presto.operator.InMemoryWindowOperator;
import com.facebook.presto.operator.JoinBuildSourceSupplierFactory;
import com.facebook.presto.operator.LimitOperator;
import com.facebook.presto.operator.LocalUnionOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.operator.OperatorStats;
import com.facebook.presto.operator.OutputProducingOperator;
import com.facebook.presto.operator.ProjectionFunction;
import com.facebook.presto.operator.ProjectionFunctions;
import com.facebook.presto.operator.SamplingOperator;
import com.facebook.presto.operator.SourceHashSupplier;
import com.facebook.presto.operator.SourceOperator;
import com.facebook.presto.operator.SourceSetSupplier;
import com.facebook.presto.operator.TableScanOperator;
import com.facebook.presto.operator.TableWriterOperator;
import com.facebook.presto.operator.TopNOperator;
import com.facebook.presto.operator.window.WindowFunction;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.split.DataStreamProvider;
import com.facebook.presto.sql.analyzer.Session;
import com.facebook.presto.sql.analyzer.Type;
import com.facebook.presto.sql.gen.ExpressionCompiler;
import com.facebook.presto.sql.gen.OperatorFactory;
import com.facebook.presto.sql.planner.plan.AggregationNode;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.FilterNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.LimitNode;
import com.facebook.presto.sql.planner.plan.OutputNode;
import com.facebook.presto.sql.planner.plan.PlanNode;
import com.facebook.presto.sql.planner.plan.PlanNodeId;
import com.facebook.presto.sql.planner.plan.PlanVisitor;
import com.facebook.presto.sql.planner.plan.ProjectNode;
import com.facebook.presto.sql.planner.plan.SampleNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.sql.planner.plan.SinkNode;
import com.facebook.presto.sql.planner.plan.SortNode;
import com.facebook.presto.sql.planner.plan.TableScanNode;
import com.facebook.presto.sql.planner.plan.TableWriterNode;
import com.facebook.presto.sql.planner.plan.TopNNode;
import com.facebook.presto.sql.planner.plan.UnionNode;
import com.facebook.presto.sql.planner.plan.WindowNode;
import com.facebook.presto.sql.tree.BooleanLiteral;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.ExpressionTreeRewriter;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Input;
import com.facebook.presto.sql.tree.InputReference;
import com.facebook.presto.sql.tree.QualifiedNameReference;
import com.facebook.presto.sql.tree.SortItem;
import com.facebook.presto.tuple.FieldOrderedTupleComparator;
import com.facebook.presto.tuple.TupleInfo;
import com.facebook.presto.tuple.TupleReadable;
import com.facebook.presto.util.IterableTransformer;
import com.facebook.presto.util.MoreFunctions;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Preconditions;
import com.google.common.base.Supplier;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;
import io.airlift.log.Logger;
import io.airlift.node.NodeInfo;

import javax.inject.Inject;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.leftGetter;
import static com.facebook.presto.sql.planner.plan.JoinNode.EquiJoinClause.rightGetter;
import static com.facebook.presto.sql.tree.Input.fieldGetter;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.equalTo;
import static com.google.common.base.Predicates.not;

public class LocalExecutionPlanner
{
    private static final Logger log = Logger.get(LocalExecutionPlanner.class);

    private final NodeInfo nodeInfo;
    private final Metadata metadata;

    private final DataStreamProvider dataStreamProvider;
    private final LocalStorageManager storageManager;
    private final Supplier<ExchangeClient> exchangeClientSupplier;
    private final ExpressionCompiler compiler;

    @Inject
    public LocalExecutionPlanner(NodeInfo nodeInfo,
            Metadata metadata,
            DataStreamProvider dataStreamProvider,
            LocalStorageManager storageManager,
            Supplier<ExchangeClient> exchangeClientSupplier,
            ExpressionCompiler compiler)
    {
        this.nodeInfo = checkNotNull(nodeInfo, "nodeInfo is null");
        this.dataStreamProvider = dataStreamProvider;
        this.exchangeClientSupplier = exchangeClientSupplier;
        this.metadata = checkNotNull(metadata, "metadata is null");
        this.storageManager = checkNotNull(storageManager, "storageManager is null");
        this.compiler = checkNotNull(compiler, "compiler is null");
    }

    public LocalExecutionPlan plan(Session session,
            PlanNode plan,
            Map<Symbol, Type> types,
            JoinBuildSourceSupplierFactory joinBuildSourceSupplierFactory,
            TaskMemoryManager taskMemoryManager,
            OperatorStats operatorStats)
    {
        LocalExecutionPlanContext context = new LocalExecutionPlanContext(session, types, joinBuildSourceSupplierFactory, taskMemoryManager, operatorStats);
        Operator rootOperator = plan.accept(new Visitor(), context).getOperator();
        return new LocalExecutionPlan(rootOperator, context.getSourceOperators(), context.getOutputOperators());
    }

    private static class LocalExecutionPlanContext
    {
        private final Session session;
        private final Map<Symbol, Type> types;
        private final JoinBuildSourceSupplierFactory joinBuildSourceSupplierFactory;
        private final TaskMemoryManager taskMemoryManager;
        private final OperatorStats operatorStats;

        private final Map<PlanNodeId, SourceOperator> sourceOperators = new HashMap<>();
        private final Map<PlanNodeId, OutputProducingOperator<?>> outputOperators = new HashMap<>();

        public LocalExecutionPlanContext(Session session,
                Map<Symbol, Type> types,
                JoinBuildSourceSupplierFactory joinBuildSourceSupplierFactory,
                TaskMemoryManager taskMemoryManager,
                OperatorStats operatorStats)
        {
            this.session = session;
            this.types = types;
            this.joinBuildSourceSupplierFactory = joinBuildSourceSupplierFactory;
            this.taskMemoryManager = taskMemoryManager;
            this.operatorStats = operatorStats;
        }

        public Session getSession()
        {
            return session;
        }

        public Map<Symbol, Type> getTypes()
        {
            return types;
        }

        public JoinBuildSourceSupplierFactory getJoinBuildSourceSupplierFactory()
        {
            return joinBuildSourceSupplierFactory;
        }

        private TaskMemoryManager getTaskMemoryManager()
        {
            return taskMemoryManager;
        }

        public OperatorStats getOperatorStats()
        {
            return operatorStats;
        }

        private void addSourceOperator(PlanNode node, SourceOperator sourceOperator)
        {
            checkState(this.sourceOperators.put(node.getId(), sourceOperator) == null, "Node %s already had a source operator assigned!", node);
        }

        private void addOutputOperator(PlanNode node, OutputProducingOperator<?> outputOperator)
        {
            checkState(this.outputOperators.put(node.getId(), outputOperator) == null, "Node %s already had an output operator assigned!", node);
        }

        private Map<PlanNodeId, SourceOperator> getSourceOperators()
        {
            return ImmutableMap.copyOf(sourceOperators);
        }

        private Map<PlanNodeId, OutputProducingOperator<?>> getOutputOperators()
        {
            return ImmutableMap.copyOf(outputOperators);
        }
    }

    public static class LocalExecutionPlan
    {
        private final Operator rootOperator;
        private final Map<PlanNodeId, SourceOperator> sourceOperators;
        private final Map<PlanNodeId, OutputProducingOperator<?>> outputOperators;

        public LocalExecutionPlan(Operator rootOperator,
                Map<PlanNodeId, SourceOperator> sourceOperators,
                Map<PlanNodeId, OutputProducingOperator<?>> outputOperators)
        {
            this.rootOperator = checkNotNull(rootOperator, "rootOperator is null");
            this.sourceOperators = ImmutableMap.copyOf(checkNotNull(sourceOperators, "sourceOperators is null"));
            this.outputOperators = ImmutableMap.copyOf(checkNotNull(outputOperators, "outputOperators is null"));
        }

        public Operator getRootOperator()
        {
            return rootOperator;
        }

        public Map<PlanNodeId, SourceOperator> getSourceOperators()
        {
            return sourceOperators;
        }

        public Map<PlanNodeId, OutputProducingOperator<?>> getOutputOperators()
        {
            return outputOperators;
        }
    }

    private class Visitor
            extends PlanVisitor<LocalExecutionPlanContext, PhysicalOperation>
    {
        @Override
        public PhysicalOperation visitExchange(ExchangeNode node, LocalExecutionPlanContext context)
        {
            List<TupleInfo> tupleInfo = getSourceOperatorTupleInfos(node, context.getTypes());

            SourceOperator operator = new ExchangeOperator(exchangeClientSupplier, tupleInfo);
            context.addSourceOperator(node, operator);

            // Fow now, we assume that remote plans always produce one symbol per channel. TODO: remove this assumption
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel, 0)); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(operator, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitOutput(OutputNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            // see if we need to introduce a projection
            //   1. verify that there's one symbol per channel
            //   2. verify that symbols from "source" match the expected order of columns according to OutputNode
            Ordering<Input> comparator = inputOrdering();

            List<Symbol> sourceSymbols = IterableTransformer.on(source.getLayout().entries())
                    .orderBy(comparator.onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list();

            List<Symbol> resultSymbols = node.getOutputSymbols();
            if (resultSymbols.equals(sourceSymbols) && resultSymbols.size() == source.getOperator().getChannelCount()) {
                // no projection needed
                return source;
            }

            // otherwise, introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(resultSymbols, source.getLayout(), context.getTypes());

            FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitWindow(WindowNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> partitionBySymbols = node.getPartitionBy();
            List<Symbol> orderBySymbols = node.getOrderBy();

            // sort by PARTITION BY, then by ORDER BY
            List<Symbol> orderingSymbols = ImmutableList.copyOf(Iterables.concat(partitionBySymbols, orderBySymbols));

            // insert a projection to put all the sort fields in a single channel if necessary
            if (!orderingSymbols.isEmpty()) {
                source = packIfNecessary(orderingSymbols, source, context.getTypes());
            }

            // find channel that fields were packed into if there is an ordering
            int orderByChannel = 0;
            if (!orderingSymbols.isEmpty()) {
                orderByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(orderingSymbols, source.getLayout()));
            }

            int[] partitionFields = new int[partitionBySymbols.size()];
            for (int i = 0; i < partitionFields.length; i++) {
                Symbol symbol = partitionBySymbols.get(i);
                partitionFields[i] = getFirst(source.getLayout().get(symbol)).getField();
            }

            int[] sortFields = new int[orderBySymbols.size()];
            boolean[] sortOrder = new boolean[orderBySymbols.size()];
            for (int i = 0; i < sortFields.length; i++) {
                Symbol symbol = orderBySymbols.get(i);
                sortFields[i] = getFirst(source.getLayout().get(symbol)).getField();
                sortOrder[i] = (node.getOrderings().get(symbol) == SortItem.Ordering.ASCENDING);
            }

            int[] outputChannels = new int[source.getOperator().getChannelCount()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            ImmutableList.Builder<WindowFunction> windowFunctions = ImmutableList.builder();
            List<Symbol> windowFunctionOutputSymbols = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getWindowFunctions().entrySet()) {
                Symbol symbol = entry.getKey();
                FunctionHandle handle = node.getFunctionHandles().get(symbol);
                windowFunctions.add(metadata.getFunction(handle).getWindowFunction().get());
                windowFunctionOutputSymbols.add(symbol);
            }

            // compute the layout of the output from the window operator
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            for (Symbol symbol : node.getSource().getOutputSymbols()) {
                outputMappings.putAll(symbol, source.getLayout().get(symbol));
            }

            // window functions go in remaining channels starting after the last channel from the source operator, one per channel
            int channel = source.getOperator().getChannelCount();
            for (Symbol symbol : windowFunctionOutputSymbols) {
                outputMappings.put(symbol, new Input(channel, 0));
                channel++;
            }

            Operator operator = new InMemoryWindowOperator(
                    source.getOperator(),
                    orderByChannel,
                    outputChannels,
                    windowFunctions.build(),
                    partitionFields,
                    sortFields,
                    sortOrder,
                    1_000_000,
                    context.getTaskMemoryManager());

            return new PhysicalOperation(operator, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitTopN(TopNNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            // insert a projection to put all the sort fields in a single channel if necessary
            source = packIfNecessary(orderBySymbols, source, context.getTypes());

            int orderByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(orderBySymbols, source.getLayout()));

            List<Integer> sortFields = new ArrayList<>();
            List<SortItem.Ordering> sortOrders = new ArrayList<>();
            for (Symbol symbol : orderBySymbols) {
                sortFields.add(getFirst(source.getLayout().get(symbol)).getField());
                sortOrders.add(node.getOrderings().get(symbol));
            }

            Ordering<TupleReadable> ordering = Ordering.from(new FieldOrderedTupleComparator(sortFields, sortOrders));

            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());

            TopNOperator operator = new TopNOperator(
                    source.getOperator(),
                    (int) node.getCount(),
                    orderByChannel,
                    mappings.getProjections(),
                    ordering,
                    node.isPartial(),
                    context.getTaskMemoryManager());
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitSort(SortNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            List<Symbol> orderBySymbols = node.getOrderBy();

            // insert a projection to put all the sort fields in a single channel if necessary
            source = packIfNecessary(orderBySymbols, source, context.getTypes());

            int orderByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(orderBySymbols, source.getLayout()));

            int[] sortFields = new int[orderBySymbols.size()];
            boolean[] sortOrder = new boolean[orderBySymbols.size()];
            for (int i = 0; i < sortFields.length; i++) {
                Symbol symbol = orderBySymbols.get(i);

                sortFields[i] = getFirst(source.getLayout().get(symbol)).getField();
                sortOrder[i] = (node.getOrderings().get(symbol) == SortItem.Ordering.ASCENDING);
            }

            int[] outputChannels = new int[source.getOperator().getChannelCount()];
            for (int i = 0; i < outputChannels.length; i++) {
                outputChannels[i] = i;
            }

            Operator operator = new InMemoryOrderByOperator(
                    source.getOperator(),
                    orderByChannel,
                    outputChannels,
                    10_000,
                    sortFields,
                    sortOrder,
                    context.getTaskMemoryManager());

            return new PhysicalOperation(operator, source.getLayout());
        }

        @Override
        public PhysicalOperation visitLimit(LimitNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            return new PhysicalOperation(new LimitOperator(source.getOperator(), node.getCount()), source.getLayout());
        }

        @Override
        public PhysicalOperation visitSample(SampleNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());
            Operator operator = new SamplingOperator(source.getOperator(), node.getSamplePercentage(), mappings.getProjections());
            return new PhysicalOperation(operator, mappings.getOutputLayout());
        }

        @Override
        public PhysicalOperation visitAggregation(AggregationNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            if (node.getGroupBy().isEmpty()) {
                return planGlobalAggregation(node, source);
            }

            return planGroupByAggregation(node, source, context);
        }

        @Override
        public PhysicalOperation visitFilter(FilterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            Map<Input, Type> inputTypes = getInputTypes(source.getLayout(), source.getOperator().getTupleInfos());

            try {
                Expression filterExpression = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(convertLayoutToInputMap(source.getLayout())), node.getPredicate());

                ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
                List<Expression> projections = new ArrayList<>();
                for (int i = 0; i < node.getOutputSymbols().size(); i++) {
                    Symbol symbol = node.getOutputSymbols().get(i);

                    Input input = getFirst(source.getLayout().get(symbol));
                    Preconditions.checkArgument(input != null, "Cannot resolve symbol %s", symbol.getName());

                    projections.add(new InputReference(input));
                    outputMappings.put(symbol, new Input(i, 0)); // one field per channel
                }

                OperatorFactory operatorFactory = compiler.compileFilterAndProjectOperator(filterExpression, projections, inputTypes);

                Operator operator = operatorFactory.createOperator(source.getOperator(), context.getSession());
                return new PhysicalOperation(operator, outputMappings.build());
            }
            catch (Exception e) {
                // compilation failed, use interpreter
                log.error(e, "Compile failed for filter=%s inputTypes=%s error=%s", node.getPredicate(), inputTypes, e);

                FilterFunction filter = new InterpretedFilterFunction(node.getPredicate(), convertLayoutToInputMap(source.getLayout()), metadata, context.getSession(), inputTypes);
                IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());
                Operator operator = new FilterAndProjectOperator(source.getOperator(), filter, mappings.getProjections());
                return new PhysicalOperation(operator, mappings.getOutputLayout());
            }
        }

        @Override
        public PhysicalOperation visitProject(ProjectNode node, LocalExecutionPlanContext context)
        {
            // collapse project on filter to a single operator
            PhysicalOperation source;
            Expression filterExpression;
            if (node.getSource() instanceof FilterNode) {
                FilterNode filterNode = (FilterNode) node.getSource();
                source = filterNode.getSource().accept(this, context);
                filterExpression = filterNode.getPredicate();
            }
            else {
                source = node.getSource().accept(this, context);
                filterExpression = BooleanLiteral.TRUE_LITERAL;
            }

            List<Expression> expressions = node.getExpressions();
            Map<Input, Type> inputTypes = null;
            try {
                Expression rewrittenFilterExpression = ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(convertLayoutToInputMap(source.getLayout())), filterExpression);

                ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
                List<Expression> projections = new ArrayList<>();
                for (int i = 0; i < expressions.size(); i++) {
                    Symbol symbol = node.getOutputSymbols().get(i);
                    projections.add(ExpressionTreeRewriter.rewriteWith(new SymbolToInputRewriter(convertLayoutToInputMap(source.getLayout())), expressions.get(i)));
                    outputMappings.put(symbol, new Input(i, 0)); // one field per channel
                }

                inputTypes = getInputTypes(source.getLayout(), source.getOperator().getTupleInfos());
                OperatorFactory operatorFactory = compiler.compileFilterAndProjectOperator(rewrittenFilterExpression, projections, inputTypes);
                Operator operator = operatorFactory.createOperator(source.getOperator(), context.getSession());
                return new PhysicalOperation(operator, outputMappings.build());
            }
            catch (Exception e) {
                // compilation failed, use interpreter
                log.error(e, "Compile failed for filter=%s projections=%s inputTypes=%s error=%s", filterExpression, node.getExpressions(), inputTypes, e);

                FilterFunction filter;
                if (filterExpression != BooleanLiteral.TRUE_LITERAL) {
                    filter = new InterpretedFilterFunction(filterExpression, convertLayoutToInputMap(source.getLayout()), metadata, context.getSession(), inputTypes);
                }
                else {
                    filter = FilterFunctions.TRUE_FUNCTION;
                }

                ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
                List<ProjectionFunction> projections = new ArrayList<>();
                for (int i = 0; i < expressions.size(); i++) {
                    Symbol symbol = node.getOutputSymbols().get(i);
                    Expression expression = expressions.get(i);

                    ProjectionFunction function;
                    if (expression instanceof QualifiedNameReference) {
                        // fast path when we know it's a direct symbol reference
                        Symbol reference = Symbol.fromQualifiedName(((QualifiedNameReference) expression).getName());
                        function = ProjectionFunctions.singleColumn(context.getTypes().get(reference).getRawType(), getFirst(source.getLayout().get(symbol)));
                    }
                    else {
                        function = new InterpretedProjectionFunction(context.getTypes().get(symbol), expression, convertLayoutToInputMap(source.getLayout()), metadata, context.getSession(), inputTypes);
                    }
                    projections.add(function);

                    outputMappings.put(symbol, new Input(i, 0)); // one field per channel
                }

                FilterAndProjectOperator operator = new FilterAndProjectOperator(source.getOperator(), filter, projections);
                return new PhysicalOperation(operator, outputMappings.build());
            }
        }

        private Map<Input, Type> getInputTypes(Multimap<Symbol, Input> layout, List<TupleInfo> tupleInfos)
        {
            Builder<Input, Type> inputTypes = ImmutableMap.builder();
            for (Input input : ImmutableSet.copyOf(layout.values())) {
                TupleInfo.Type type = tupleInfos.get(input.getChannel()).getTypes().get(input.getField());
                switch (type) {
                    case BOOLEAN:
                        inputTypes.put(input, Type.BOOLEAN);
                        break;
                    case FIXED_INT_64:
                        inputTypes.put(input, Type.LONG);
                        break;
                    case VARIABLE_BINARY:
                        inputTypes.put(input, Type.STRING);
                        break;
                    case DOUBLE:
                        inputTypes.put(input, Type.DOUBLE);
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported type " + type);
                }
            }
            return inputTypes.build();
        }

        @Override
        public PhysicalOperation visitTableScan(TableScanNode node, LocalExecutionPlanContext context)
        {
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            List<ColumnHandle> columns = new ArrayList<>();

            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                columns.add(node.getAssignments().get(symbol));

                outputMappings.put(symbol, new Input(channel, 0)); // one column per channel
                channel++;
            }

            List<TupleInfo> tupleInfos = getSourceOperatorTupleInfos(node, context.getTypes());
            TableScanOperator operator = new TableScanOperator(dataStreamProvider, tupleInfos, columns);
            context.addSourceOperator(node, operator);
            return new PhysicalOperation(operator, outputMappings.build());
        }

        @Override
        public PhysicalOperation visitJoin(JoinNode node, LocalExecutionPlanContext context)
        {
            List<JoinNode.EquiJoinClause> clauses = node.getCriteria();

            // introduce a projection to put all fields from the left side into a single channel if necessary
            PhysicalOperation leftSource = node.getLeft().accept(this, context);
            List<Symbol> leftSymbols = Lists.transform(clauses, leftGetter());

            // do the same on the right side
            PhysicalOperation rightSource = node.getRight().accept(this, context);
            List<Symbol> rightSymbols = Lists.transform(clauses, rightGetter());

            switch (node.getType())
            {
                case INNER:
                case LEFT:
                    return createJoinOperator(node, leftSource, leftSymbols, rightSource, rightSymbols, context);
                case RIGHT:
                    return createJoinOperator(node, rightSource, rightSymbols, leftSource, leftSymbols, context);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + node.getType());
            }
        }

        private PhysicalOperation createJoinOperator(JoinNode node, PhysicalOperation probeSource, List<Symbol> probeSymbols, PhysicalOperation buildSource, List<Symbol> buildSymbols, LocalExecutionPlanContext context)
        {
            // introduce a projection to put all fields from the probe side into a single channel if necessary
            probeSource = packIfNecessary(probeSymbols, probeSource, context.getTypes());

            // do the same on the build side
            buildSource = packIfNecessary(buildSymbols, buildSource, context.getTypes());

            int probeChannel = Iterables.getOnlyElement(getChannelSetForSymbols(probeSymbols, probeSource.getLayout()));
            int buildChannel = Iterables.getOnlyElement(getChannelSetForSymbols(buildSymbols, buildSource.getLayout()));

            JoinBuildSourceSupplierFactory joinBuildSourceSupplierFactory = context.getJoinBuildSourceSupplierFactory();

            // Probe channels are always laid out first
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            outputMappings.putAll(probeSource.getLayout());

            // inputs from build side of the join are laid out following the input from the probe side,
            // so adjust the channel ids but keep the field layouts intact
            int offset = probeSource.getOperator().getChannelCount();
            for (Map.Entry<Symbol, Input> entry : buildSource.getLayout().entries()) {
                Input input = entry.getValue();
                outputMappings.put(entry.getKey(), new Input(offset + input.getChannel(), input.getField()));
            }

            SourceHashSupplier hashSupplier = joinBuildSourceSupplierFactory.getSourceHashSupplier(node, buildSource.getOperator(), buildChannel, context.getOperatorStats());
            Operator operator = createHashJoinOperator(node.getType(), hashSupplier, probeSource.getOperator(), probeChannel);

            return new PhysicalOperation(operator, outputMappings.build());
        }

        private HashJoinOperator createHashJoinOperator(JoinNode.Type type, SourceHashSupplier hashSupplier, Operator probeSource, int probeJoinChannel)
        {
            switch (type) {
                case INNER:
                    return HashJoinOperator.innerJoin(hashSupplier, probeSource, probeJoinChannel);
                case LEFT:
                case RIGHT:
                    return HashJoinOperator.outerjoin(hashSupplier, probeSource, probeJoinChannel);
                default:
                    throw new UnsupportedOperationException("Unsupported join type: " + type);
            }
        }

        @Override
        public PhysicalOperation visitSemiJoin(SemiJoinNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);
            PhysicalOperation filteringSource = node.getFilteringSource().accept(this, context);

            // introduce a projection to put all fields from the probe side into a single channel if necessary
            source = packIfNecessary(ImmutableList.of(node.getSourceJoinSymbol()), source, context.getTypes());

            // do the same on the build side
            filteringSource = packIfNecessary(ImmutableList.of(node.getFilteringSourceJoinSymbol()), filteringSource, context.getTypes());

            int probeChannel = Iterables.getOnlyElement(getChannelSetForSymbols(ImmutableList.of(node.getSourceJoinSymbol()), source.getLayout()));
            int buildChannel = Iterables.getOnlyElement(getChannelSetForSymbols(ImmutableList.of(node.getFilteringSourceJoinSymbol()), filteringSource.getLayout()));

            JoinBuildSourceSupplierFactory joinBuildSourceSupplierFactory = context.getJoinBuildSourceSupplierFactory();

            int offset = source.getOperator().getChannelCount();

            // Source channels are always laid out first, followed by the boolean output symbol
            ImmutableMultimap<Symbol, Input> outputMappings = ImmutableMultimap.<Symbol, Input>builder()
                    .putAll(source.getLayout())
                    .put(node.getSemiJoinOutput(), new Input(offset, 0))
                    .build();

            SourceSetSupplier setProvider = joinBuildSourceSupplierFactory.getSourceSetSupplier(node, filteringSource.getOperator(), buildChannel, context.getOperatorStats());
            Operator operator = new HashSemiJoinOperator(source.getOperator(), probeChannel, setProvider);

            return new PhysicalOperation(operator, outputMappings);
        }

        @Override
        public PhysicalOperation visitSink(SinkNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation source = node.getSource().accept(this, context);

            boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entries())
                    .orderBy(inputOrdering().onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                    .transform(MoreFunctions.<Symbol, Input>keyGetter())
                    .list()
                    .equals(node.getOutputSymbols());

            // if any symbols are mapped to a non-zero field, re-map to one field per channel
            // TODO: this is currently what the exchange operator expects -- figure out how to remove this assumption
            // to avoid unnecessary projections
            boolean hasMultiFieldChannels = IterableTransformer.on(source.getLayout().values())
                    .transform(fieldGetter())
                    .any(not(equalTo(0)));

            if (hasMultiFieldChannels || !projectionMatchesOutput) {
                IdentityProjectionInfo mappings = computeIdentityMapping(node.getOutputSymbols(), source.getLayout(), context.getTypes());
                Operator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());
                // NOTE: the generated output layout may not be completely accurate if the same field was projected as multiple inputs.
                // However, this should not affect the operation of the sink.
                return new PhysicalOperation(operator, mappings.getOutputLayout());
            }

            return source;
        }

        @Override
        public PhysicalOperation visitTableWriter(TableWriterNode node, LocalExecutionPlanContext context)
        {
            PhysicalOperation query = node.getSource().accept(this, context);

            ImmutableList.Builder<ColumnHandle> columns = ImmutableList.builder();
            ImmutableList.Builder<Symbol> symbols = ImmutableList.builder();
            for (Map.Entry<Symbol, ColumnHandle> entry : node.getColumns().entrySet()) {
                symbols.add(entry.getKey());
                columns.add(entry.getValue());
            }


            // introduce a projection to match the expected output
            IdentityProjectionInfo mappings = computeIdentityMapping(symbols.build(), query.getLayout(), context.getTypes());
            Operator sourceOperator = new FilterAndProjectOperator(query.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections());

            Symbol outputSymbol = Iterables.getOnlyElement(node.getOutputSymbols());
            TableWriterOperator operator = new TableWriterOperator(storageManager,
                    nodeInfo.getNodeId(),
                    columns.build(),
                    sourceOperator);

            context.addSourceOperator(node, operator);
            context.addOutputOperator(node, operator);

            return new PhysicalOperation(operator,
                    ImmutableMultimap.of(outputSymbol, new Input(0, 0)));
        }

        @Override
        public PhysicalOperation visitUnion(UnionNode node, LocalExecutionPlanContext context)
        {
            ImmutableList.Builder<Operator> operatorBuilder = ImmutableList.builder();
            for (int i = 0; i < node.getSources().size(); i++) {
                PlanNode subplan = node.getSources().get(i);
                List<Symbol> expectedLayout = node.sourceOutputLayout(i);

                PhysicalOperation source = subplan.accept(this, context);

                boolean projectionMatchesOutput = IterableTransformer.on(source.getLayout().entries())
                        .orderBy(inputOrdering().onResultOf(MoreFunctions.<Symbol, Input>valueGetter()))
                        .transform(MoreFunctions.<Symbol, Input>keyGetter())
                        .list()
                        .equals(expectedLayout);

                if (!projectionMatchesOutput) {
                    IdentityProjectionInfo mappings = computeIdentityMapping(expectedLayout, source.getLayout(), context.getTypes());
                    operatorBuilder.add(new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, mappings.getProjections()));
                }
                else {
                    operatorBuilder.add(source.getOperator());
                }
            }

            // Fow now, we assume that subplans always produce one symbol per channel. TODO: remove this assumption
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            int channel = 0;
            for (Symbol symbol : node.getOutputSymbols()) {
                outputMappings.put(symbol, new Input(channel, 0)); // one symbol per channel
                channel++;
            }

            return new PhysicalOperation(new LocalUnionOperator(operatorBuilder.build()), outputMappings.build());
        }

        @Override
        protected PhysicalOperation visitPlan(PlanNode node, LocalExecutionPlanContext context)
        {
            throw new UnsupportedOperationException("not yet implemented");
        }

        private List<TupleInfo> getSourceOperatorTupleInfos(PlanNode node, Map<Symbol, Type> types)
        {
            // Fow now, we assume that remote plans always produce one symbol per channel. TODO: remove this assumption
            return ImmutableList.copyOf(IterableTransformer.on(node.getOutputSymbols())
                    .transform(Functions.forMap(types))
                    .transform(Type.toRaw())
                    .transform(new Function<TupleInfo.Type, TupleInfo>()
                    {
                        @Override
                        public TupleInfo apply(TupleInfo.Type input)
                        {
                            return new TupleInfo(input);
                        }
                    })
                    .list());
        }

        private AggregationFunctionDefinition buildFunctionDefinition(PhysicalOperation source, FunctionHandle function, FunctionCall call)
        {
            List<Input> arguments = new ArrayList<>();
            for (Expression argument : call.getArguments()) {
                Symbol argumentSymbol = Symbol.fromQualifiedName(((QualifiedNameReference) argument).getName());
                arguments.add(getFirst(source.getLayout().get(argumentSymbol)));
            }

            return metadata.getFunction(function).bind(arguments);
        }

        private PhysicalOperation planGlobalAggregation(AggregationNode node, PhysicalOperation source)
        {
            int outputChannel = 0;
            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                outputMappings.put(symbol, new Input(outputChannel, 0)); // one aggregation per channel
                outputChannel++;
            }

            Operator operator = new AggregationOperator(source.getOperator(), node.getStep(), functionDefinitions);
            return new PhysicalOperation(operator, outputMappings.build());
        }

        private PhysicalOperation planGroupByAggregation(AggregationNode node, PhysicalOperation source, LocalExecutionPlanContext context)
        {
            List<Symbol> groupBySymbols = node.getGroupBy();

            // introduce a projection to put all group by fields from the source into a single channel if necessary
            source = packIfNecessary(groupBySymbols, source, context.getTypes());

            List<Symbol> aggregationOutputSymbols = new ArrayList<>();
            List<AggregationFunctionDefinition> functionDefinitions = new ArrayList<>();
            for (Map.Entry<Symbol, FunctionCall> entry : node.getAggregations().entrySet()) {
                Symbol symbol = entry.getKey();

                functionDefinitions.add(buildFunctionDefinition(source, node.getFunctions().get(symbol), entry.getValue()));
                aggregationOutputSymbols.add(symbol);
            }

            ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
            // add group-by key fields. They all go in channel 0 in the same order produced by the source operator
            for (Symbol symbol : groupBySymbols) {
                outputMappings.put(symbol, new Input(0, getFirst(source.getLayout().get(symbol)).getField()));
            }

            // aggregations go in remaining channels starting at 1, one per channel
            int channel = 1;
            for (Symbol symbol : aggregationOutputSymbols) {
                outputMappings.put(symbol, new Input(channel, 0));
                channel++;
            }

            Integer groupByChannel = Iterables.getOnlyElement(getChannelSetForSymbols(groupBySymbols, source.getLayout()));
            Operator aggregationOperator = new HashAggregationOperator(
                    source.getOperator(),
                    groupByChannel,
                    node.getStep(),
                    functionDefinitions,
                    10_000,
                    context.getTaskMemoryManager());

            return new PhysicalOperation(aggregationOperator, outputMappings.build());
        }
    }

    private static IdentityProjectionInfo computeIdentityMapping(List<Symbol> symbols, Multimap<Symbol, Input> inputLayout, Map<Symbol, Type> types)
    {
        ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
        List<ProjectionFunction> projections = new ArrayList<>();

        int channel = 0;
        for (Symbol symbol : symbols) {
            ProjectionFunction function = ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), getFirst(inputLayout.get(symbol)));
            projections.add(function);
            outputMappings.put(symbol, new Input(channel, 0)); // one field per channel
            channel++;
        }

        return new IdentityProjectionInfo(outputMappings.build(), projections);
    }

    /**
     * Inserts a projection if the provided symbols are not in a single channel by themselves
     */
    private PhysicalOperation packIfNecessary(List<Symbol> symbols, PhysicalOperation source, Map<Symbol, Type> types)
    {
        List<Integer> channels = getChannelsForSymbols(symbols, source.getLayout());
        List<TupleInfo> tupleInfos = source.getOperator().getTupleInfos();
        if (channels.size() > 1 || tupleInfos.get(Iterables.getOnlyElement(channels)).getFieldCount() > 1) {
            source = pack(source, symbols, types);
        }
        return source;
    }

    /**
     * Inserts a Projection that places the requested symbols into the same channel in the order specified
     */
    private static PhysicalOperation pack(PhysicalOperation source, List<Symbol> symbols, Map<Symbol, Type> types)
    {
        Preconditions.checkArgument(!symbols.isEmpty(), "symbols is empty");

        List<Symbol> otherSymbols = ImmutableList.copyOf(Sets.difference(source.getLayout().keySet(), ImmutableSet.copyOf(symbols)));

        // split composite channels into one field per channel. TODO: Fix it so that it preserves the layout of channels for "otherSymbols"
        IdentityProjectionInfo mappings = computeIdentityMapping(otherSymbols, source.getLayout(), types);

        ImmutableMultimap.Builder<Symbol, Input> outputMappings = ImmutableMultimap.builder();
        ImmutableList.Builder<ProjectionFunction> projections = ImmutableList.builder();

        outputMappings.putAll(mappings.getOutputLayout());
        projections.addAll(mappings.getProjections());

        // append a projection that packs all the input symbols into a single channel (it goes in the last channel)
        List<ProjectionFunction> packedProjections = new ArrayList<>();
        int channel = mappings.getProjections().size();
        int field = 0;
        for (Symbol symbol : symbols) {
            packedProjections.add(ProjectionFunctions.singleColumn(types.get(symbol).getRawType(), getFirst(source.getLayout().get(symbol))));
            outputMappings.put(symbol, new Input(channel, field));
            field++;
        }
        projections.add(ProjectionFunctions.concat(packedProjections));

        Operator operator = new FilterAndProjectOperator(source.getOperator(), FilterFunctions.TRUE_FUNCTION, projections.build());
        return new PhysicalOperation(operator, outputMappings.build());
    }

    private static List<Integer> getChannelsForSymbols(List<Symbol> symbols, Multimap<Symbol, Input> layout)
    {
        ImmutableList.Builder<Integer> builder = ImmutableList.builder();
        for (Symbol symbol : symbols) {
            builder.add(getFirst(layout.get(symbol)).getChannel());
        }
        return builder.build();
    }

    private static Set<Integer> getChannelSetForSymbols(List<Symbol> symbols, Multimap<Symbol, Input> layout)
    {
        return ImmutableSet.copyOf(getChannelsForSymbols(symbols, layout));
    }

    private static Map<Symbol, Input> convertLayoutToInputMap(Multimap<Symbol, Input> layout)
    {
        Builder<Symbol, Input> builder = ImmutableMap.builder();
        for (Map.Entry<Symbol, Collection<Input>> entry : layout.asMap().entrySet()) {
            builder.put(entry.getKey(), getFirst(entry.getValue()));
        }
        return builder.build();
    }

    private static <T> T getFirst(Iterable<T> iterable)
    {
        return iterable.iterator().next();
    }

    private static class IdentityProjectionInfo
    {
        private final Multimap<Symbol, Input> layout;
        private final List<ProjectionFunction> projections;

        public IdentityProjectionInfo(Multimap<Symbol, Input> outputLayout, List<ProjectionFunction> projections)
        {
            this.layout = checkNotNull(outputLayout, "outputLayout is null");
            this.projections = checkNotNull(projections, "projections is null");
        }

        public Multimap<Symbol, Input> getOutputLayout()
        {
            return layout;
        }

        public List<ProjectionFunction> getProjections()
        {
            return projections;
        }
    }

    /**
     * Encapsulates an physical operator plus the mapping of logical symbols to channel/field
     */
    private static class PhysicalOperation
    {
        private Operator operator;
        private Multimap<Symbol, Input> layout;

        public PhysicalOperation(Operator operator, Multimap<Symbol, Input> layout)
        {
            checkNotNull(operator, "operator is null");
            Preconditions.checkNotNull(layout, "layout is null");

            this.operator = operator;
            this.layout = layout;
        }

        public Operator getOperator()
        {
            return operator;
        }

        public Multimap<Symbol, Input> getLayout()
        {
            return layout;
        }
    }

    private static Ordering<Input> inputOrdering()
    {
        return Ordering.from(new Comparator<Input>()
        {
            @Override
            public int compare(Input o1, Input o2)
            {
                return ComparisonChain.start()
                        .compare(o1.getChannel(), o2.getChannel())
                        .compare(o1.getField(), o2.getField())
                        .result();
            }
        });
    }
}
