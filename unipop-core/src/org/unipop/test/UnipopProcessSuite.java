package org.unipop.test;

import org.apache.tinkerpop.gremlin.process.ProcessStandardSuite;
import org.apache.tinkerpop.gremlin.process.traversal.CoreTraversalTest;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalSideEffectsTest;
import org.apache.tinkerpop.gremlin.process.traversal.step.branch.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.filter.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.map.*;
import org.apache.tinkerpop.gremlin.process.traversal.step.sideEffect.*;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.ElementIdStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.EventStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.PartitionStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.decoration.SubgraphStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ComputerVerificationStrategyProcessTest;
import org.apache.tinkerpop.gremlin.process.traversal.strategy.verification.ReadOnlyStrategyProcessTest;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;

public class UnipopProcessSuite extends ProcessStandardSuite {
    public UnipopProcessSuite(final Class<?> klass, final RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests);
    }

    private static final Class<?>[] allTests = new Class<?>[]{
            // branch
            BranchTest.Traversals.class,
            ChooseTest.Traversals.class,
            LocalTest.Traversals.class,
            RepeatTest.Traversals.class,
            UnionTest.Traversals.class,

            // filter
            AndTest.Traversals.class,
            CoinTest.Traversals.class,
            CyclicPathTest.Traversals.class,
            DedupTest.Traversals.class,
            DropTest.Traversals.class,
            FilterTest.Traversals.class,
            HasTest.Traversals.class,
            IsTest.Traversals.class,
            OrTest.Traversals.class,
            RangeTest.Traversals.class,
            SampleTest.Traversals.class,
            SimplePathTest.Traversals.class,
            TailTest.Traversals.class,
            WhereTest.Traversals.class,

            // map
            AddEdgeTest.Traversals.class,
            AddVertexTest.Traversals.class,
            CoalesceTest.Traversals.class,
            ConstantTest.Traversals.class,
            CountTest.Traversals.class,
            FlatMapTest.Traversals.class,
            FoldTest.Traversals.class,
            GraphTest.Traversals.class,
            LoopsTest.Traversals.class,
            MapTest.Traversals.class,
            MapKeysTest.Traversals.class,
            MapValuesTest.Traversals.class,
            MatchTest.CountMatchTraversals.class,
            MatchTest.GreedyMatchTraversals.class,
            MaxTest.Traversals.class,
            MeanTest.Traversals.class,
            MinTest.Traversals.class,
            SumTest.Traversals.class,
            OrderTest.Traversals.class,
            PathTest.Traversals.class,
            PropertiesTest.Traversals.class,
            SelectTest.Traversals.class,
            VertexTest.Traversals.class,
            UnfoldTest.Traversals.class,
            ValueMapTest.Traversals.class,

            // sideEffect
            AggregateTest.Traversals.class,
            ExplainTest.Traversals.class,
            GroupTest.Traversals.class,
            GroupTestV3d0.Traversals.class,
            GroupCountTest.Traversals.class,
            InjectTest.Traversals.class,
            ProfileTest.Traversals.class,
            SackTest.Traversals.class,
            SideEffectCapTest.Traversals.class,
            SideEffectTest.Traversals.class,
            StoreTest.Traversals.class,
            SubgraphTest.Traversals.class,
            TreeTest.Traversals.class,

            // util
            TraversalSideEffectsTest.Traversals.class,

            // compliance
            CoreTraversalTest.class,

            // strategy
            ComputerVerificationStrategyProcessTest.StandardTraversals.class,

            // algorithms
            // PageRankVertexProgramTest.class

            // decorations
            ElementIdStrategyProcessTest.class,
            EventStrategyProcessTest.class,
            ReadOnlyStrategyProcessTest.class,
            PartitionStrategyProcessTest.class,
            SubgraphStrategyProcessTest.class
    };
}
