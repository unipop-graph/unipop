package org.unipop.rest.suite;

import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Stage;
import io.cucumber.guice.CucumberModules;
import io.cucumber.junit.Cucumber;
import io.cucumber.junit.CucumberOptions;
import org.apache.tinkerpop.gremlin.features.AbstractGuiceFactory;
import org.apache.tinkerpop.gremlin.features.World;
import org.junit.runner.RunWith;

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
        objectFactory = RestFeatureTest.RestGuiceFactory.class,
        features = { "classpath:/org/apache/tinkerpop/gremlin/test/features" },
        plugin = { "progress", "junit:target/cucumber.xml" })
public class RestFeatureTest {

    public static final class RestGuiceFactory extends AbstractGuiceFactory {
        public RestGuiceFactory() {
            super(Guice.createInjector(Stage.PRODUCTION, CucumberModules.createScenarioModule(), new ServiceModule()));
        }
    }

    public static final class ServiceModule extends AbstractModule {
        @Override protected void configure() { bind(World.class).to(RestWorld.class); }
    }
}
