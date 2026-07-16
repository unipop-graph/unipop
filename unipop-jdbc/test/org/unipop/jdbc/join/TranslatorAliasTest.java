package org.unipop.jdbc.join;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.step.util.HasContainer;
import org.jooq.Condition;
import org.jooq.SQLDialect;
import org.jooq.impl.DSL;
import org.junit.Test;
import org.unipop.jdbc.utils.JdbcPredicatesTranslator;
import org.unipop.query.predicates.PredicatesHolderFactory;

import java.util.Collections;

import static org.junit.Assert.assertTrue;

public class TranslatorAliasTest {

    @Test
    public void aliasQualifiesColumns() {
        JdbcPredicatesTranslator t = new JdbcPredicatesTranslator(
                Collections.emptySet(), Collections.emptySet(), Collections.emptySet(),
                Collections.emptySet(), Collections.emptySet(), "v");
        Condition c = t.translate(PredicatesHolderFactory.predicate(new HasContainer("status", P.eq("open"))));
        String sql = DSL.using(SQLDialect.POSTGRES).renderInlined(c);
        assertTrue("expected qualified column, got: " + sql, sql.contains("\"v\".\"status\""));
    }

    @Test
    public void noAliasIsUnqualified() {
        JdbcPredicatesTranslator t = new JdbcPredicatesTranslator(Collections.emptySet());
        Condition c = t.translate(PredicatesHolderFactory.predicate(new HasContainer("status", P.eq("open"))));
        String sql = DSL.using(SQLDialect.POSTGRES).renderInlined(c);
        assertTrue("expected unqualified column, got: " + sql, sql.contains("status") && !sql.contains("\"v\".\"status\""));
    }
}
