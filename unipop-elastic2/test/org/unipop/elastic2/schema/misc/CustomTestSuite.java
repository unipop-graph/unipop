package org.unipop.elastic2.schema.misc;

import org.apache.tinkerpop.gremlin.AbstractGremlinSuite;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalEngine;
import org.junit.runners.model.InitializationError;
import org.junit.runners.model.RunnerBuilder;
import org.unipop.elastic2.misc.GroupTests;

/**
 * Created by Gilad on 20/10/2015.
 */
public class CustomTestSuite extends AbstractGremlinSuite {
    //private static final Class<?>[] allTests = new Class[]{DetachedGraphTest.class};
    //private static final Class<?>[] allTests = new Class[]{DetachedEdgeTest.class};
    //private static final Class<?>[] allTests = new Class[]{DetachedVertexPropertyTest.class};
    //private static final Class<?>[] allTests = new Class[]{DetachedPropertyTest.class};
    //private static final Class<?>[] allTests = new Class[]{DetachedVertexTest.class};
    //private static final Class<?>[] allTests = new Class[]{EdgeTest.class};
    //private static final Class<?>[] allTests = new Class[]{FeatureSupportTest.class};
    //private static final Class<?>[] allTests = new Class[]{IoCustomTest.class};
    //private static final Class<?>[] allTests = new Class[]{IoEdgeTest.class};
    //private static final Class<?>[] allTests = new Class[]{IoGraphTest.class};
    //private static final Class<?>[] allTests = new Class[]{IoVertexTest.class};
    //private static final Class<?>[] allTests = new Class[]{IoPropertyTest.class};
    //private static final Class<?>[] allTests = new Class[]{GraphTest.class};
    //private static final Class<?>[] allTests = new Class[]{GraphConstructionTest.class};
    //private static final Class<?>[] allTests = new Class[]{IoTest.class}; // has errors
    //private static final Class<?>[] allTests = new Class[]{VertexPropertyTest.class};
    //private static final Class<?>[] allTests = new Class[]{VariablesTest.class};
    //private static final Class<?>[] allTests = new Class[]{PropertyTest.class}; // has errors
    //private static final Class<?>[] allTests = new Class[]{ReferenceGraphTest.class};
    //private static final Class<?>[] allTests = new Class[]{ReferenceEdgeTest.class};
    //private static final Class<?>[] allTests = new Class[]{ReferenceVertexPropertyTest.class};
    //private static final Class<?>[] allTests = new Class[]{ReferenceVertexTest.class};
    //private static final Class<?>[] allTests = new Class[]{SerializationTest.class};
    //private static final Class<?>[] allTests = new Class[]{StarGraphTest.class};
    //private static final Class<?>[] allTests = new Class[]{TransactionTest.class};
    //private static final Class<?>[] allTests = new Class[]{VertexTest.class};
    //private static final Class<?>[] allTests = new Class[]{FeatureSupportTest.GraphVariablesFunctionalityTest.class};
    //private static final Class<?>[] allTests = new Class[]{FeatureSupportTest.ElementPropertyDataTypeFunctionalityTest.class};
    //private static final Class<?>[] allTests = new Class[]{VariablesTest.GraphVariablesFeatureSupportTest.class};
    //private static final Class<?>[] allTests = new Class[]{PropertyTest.PropertyFeatureSupportTest.class};
    //private static final Class<?>[] allTests = new Class[]{PropertyTest.PropertyValidationOnAddExceptionConsistencyTest.class};

    //private static final Class<?>[] allTests = new Class[]{VertexTest.Traversals.class, DetachedGraphTest.class, DetachedEdgeTest.class, DetachedVertexPropertyTest.class, DetachedPropertyTest.class, DetachedVertexTest.class, EdgeTest.class, FeatureSupportTest.class, IoCustomTest.class, IoEdgeTest.class, IoGraphTest.class, IoVertexTest.class, IoPropertyTest.class, GraphTest.class, GraphConstructionTest.class, VertexPropertyTest.class, VariablesTest.class, ReferenceGraphTest.class, ReferenceEdgeTest.class, ReferenceVertexPropertyTest.class, ReferenceVertexTest.class, SerializationTest.class, StarGraphTest.class, TransactionTest.class, VertexTest.class, FeatureSupportTest.GraphVariablesFunctionalityTest.class, FeatureSupportTest.ElementPropertyDataTypeFunctionalityTest.class, VariablesTest.GraphVariablesFeatureSupportTest.class, PropertyTest.PropertyFeatureSupportTest.class};



    //private static final Class<?>[] allTests = new Class[]{BranchTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{ChooseTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{LocalTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{RepeatTest.Traversals.class};
   // private static final Class<?>[] allTests = new Class[]{UnionTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{AndTest.Traversals.class, CoinTest.Traversals.class, CyclicPathTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{DedupTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{DropTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{FilterTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{HasTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{IsTest.Traversals.class, OrTest.Traversals.class, RangeTest.Traversals.class, SampleTest.Traversals.class, SimplePathTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{SampleTest.Traversals.class};
   // private static final Class<?>[] allTests = new Class[]{RangeTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{TailTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{WhereTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{AddEdgeTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{AddVertexTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{CoalesceTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{ConstantTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{CountTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{ CountTests.class};
    //private static final Class<?>[] allTests = new Class[]{FoldTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{MapTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{MapKeysTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{MapValuesTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{MatchTest.CountMatchTraversals.class};
    //private static final Class<?>[] allTests = new Class[]{WhereTest.Traversals.class, AddEdgeTest.Traversals.class, AddVertexTest.Traversals.class, CoalesceTest.Traversals.class, ConstantTest.Traversals.class, CountTest.Traversals.class, FoldTest.Traversals.class, MapTest.Traversals.class, MapKeysTest.Traversals.class, MapValuesTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{MatchTest.CountMatchTraversals.class};
    //private static final Class<?>[] allTests = new Class[]{MatchTest.GreedyMatchTraversals.class};
    //private static final Class<?>[] allTests = new Class[]{MaxTest.Traversals.class,MeanTest.Traversals.class,MinTest.Traversals.class,SumTest.Traversals.class,OrderTest.Traversals.class,PropertiesTest.Traversals.class};
//    private static final Class<?>[] allTests = new Class[]{ UnfoldTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ ValueMapTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ AggregateTest.Traversals.class };
   // private static final Class<?>[] allTests = new Class[]{ GroupTest.Traversals.class };
    //private static final Class<?>[] allTests = new Class[]{ GroupTests.class };
//    private static final Class<?>[] allTests = new Class[]{ GroupCountTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ InjectTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ SackTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ SideEffectCapTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ SideEffectTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ StoreTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ TreeTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ TraversalSideEffectsTest.Traversals.class };
//    private static final Class<?>[] allTests = new Class[]{ CoreTraversalTest.class};
//    private static final Class<?>[] allTests = new Class[]{ ComputerVerificationStrategyProcessTest.StandardTraversals.class };
//    private static final Class<?>[] allTests = new Class[]{ ElementIdStrategyProcessTest.class };
   // private static final Class<?>[] allTests = new Class[]{ EventStrategyProcessTest.class };
//    private static final Class<?>[] allTests = new Class[]{ ReadOnlyStrategyProcessTest.class,PartitionStrategyProcessTest.class,SubgraphStrategyProcessTest.class };
    //private static final Class<?>[] allTests = new Class[]{ PathTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{ SelectTest.Traversals.class};
    //private static final Class<?>[] allTests = new Class[]{ VertexTest.Traversals.class };
    //private static final Class<?>[] allTests = new Class[]{DistributionGeneratorTest.class};
    //private static final Class<?>[] allTests = new Class[]{org.apache.tinkerpop.gremlin.structure.VertexTest.BasicVertexTest.class};


    // problematic
    // last test runs till OutOfMemory Exception (both on ran's code and ours)
    //private static final Class<?>[] allTests = new Class[]{ MatchTest.GreedyMatchTraversals.class };
    //private static final Class<?>[] allTests = new Class[]{ ProfileTest.Traversals.class };

   //private static final Class<?>[] allTests = new Class[]{ SubgraphStrategyProcessTest.class };

    private static final Class<?>[] allTests = new Class[]{ GroupTests.class };


    //private static final Class<?>[] allTests = new Class[]{BranchTest.Traversals.class, ChooseTest.Traversals.class, LocalTest.Traversals.class, RepeatTest.Traversals.class, UnionTest.Traversals.class, AndTest.Traversals.class, CoinTest.Traversals.class, CyclicPathTest.Traversals.class, DedupTest.Traversals.class, DropTest.Traversals.class, FilterTest.Traversals.class, HasTest.Traversals.class, IsTest.Traversals.class, OrTest.Traversals.class, RangeTest.Traversals.class, SampleTest.Traversals.class, SimplePathTest.Traversals.class, TailTest.Traversals.class, WhereTest.Traversals.class, AddEdgeTest.Traversals.class, AddVertexTest.Traversals.class, CoalesceTest.Traversals.class, ConstantTest.Traversals.class, CountTest.Traversals.class, FoldTest.Traversals.class, MapTest.Traversals.class, MapKeysTest.Traversals.class, MapValuesTest.Traversals.class, MaxTest.Traversals.class,MeanTest.Traversals.class,MinTest.Traversals.class,SumTest.Traversals.class,OrderTest.Traversals.class,PropertiesTest.Traversals.class, UnfoldTest.Traversals.class ,ValueMapTest.Traversals.class ,AggregateTest.Traversals.class ,GroupTest.Traversals.class ,GroupCountTest.Traversals.class ,InjectTest.Traversals.class, SackTest.Traversals.class ,SideEffectCapTest.Traversals.class ,SideEffectTest.Traversals.class ,StoreTest.Traversals.class ,TreeTest.Traversals.class ,TraversalSideEffectsTest.Traversals.class,CoreTraversalTest.class,ComputerVerificationStrategyProcessTest.StandardTraversals.class,ElementIdStrategyProcessTest.class,EventStrategyProcessTest.class,ReadOnlyStrategyProcessTest.class,PartitionStrategyProcessTest.class,SubgraphStrategyProcessTest.class, PathTest.Traversals.class, SelectTest.Traversals.class, VertexTest.Traversals.class};



    public CustomTestSuite(Class<?> klass, RunnerBuilder builder) throws InitializationError {
        super(klass, builder, allTests, (Class[]) null, false, TraversalEngine.Type.STANDARD);
    }
}
