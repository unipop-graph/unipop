package org.unipop.jdbc.suite;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.apache.tinkerpop.gremlin.features.AbstractGuiceFactory;
import org.apache.tinkerpop.gremlin.features.World;
import org.junit.runner.RunWith;

/**
 * Cucumber runner for the TinkerPop 3.8 process-compliance feature suite, executed against the
 * JDBC (H2) Unipop provider. Replaces the removed {@code ProcessStandardSuite} JUnit process tests.
 *
 * <p>Tags are excluded for capabilities Unipop's JDBC federation provider does not support:
 * graph computer, null/list/set/map/uuid/date-time property values, meta/multi-properties,
 * and user-supplied element ids.</p>
 */
@RunWith(Cucumber.class)
@CucumberOptions(
        tags = "not @GraphComputerOnly " +
                "and not @AllowNullPropertyValues " +
                "and not @MultiProperties " +
                "and not @MetaProperties " +
                "and not @UserSuppliedVertexIds " +
                "and not @UserSuppliedEdgeIds " +
                "and not @UserSuppliedVertexPropertyIds " +
                "and not @AllowListPropertyValues " +
                "and not @AllowSetPropertyValues " +
                "and not @AllowMapPropertyValues " +
                "and not @AllowUUIDPropertyValues " +
                "and not @AllowDateTimePropertyValues",
        glue = { "org.apache.tinkerpop.gremlin.features" },
        objectFactory = JdbcFeatureTest.JdbcGuiceFactory.class,
        features = { "classpath:/org/apache/tinkerpop/gremlin/test/features" },
        plugin = { "progress", "junit:target/cucumber.xml" })
public class JdbcFeatureTest {

    public static final class JdbcGuiceFactory extends AbstractGuiceFactory {
        public JdbcGuiceFactory() {
            super(Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule()));
        }
    }

    public static final class ServiceModule extends AbstractModule {
        @Override
        protected void configure() {
            bind(World.class).to(JdbcWorld.class);
        }
    }
}
