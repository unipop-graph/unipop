package com.tinkerpop.gremlin.elastic.process.graph.traversal.sideEffect;

import com.tinkerpop.gremlin.elastic.structure.ElasticEdge;
import com.tinkerpop.gremlin.process.graph.util.HasContainer;
import com.tinkerpop.gremlin.structure.*;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.*;

import java.util.List;

/**
 * Created by Eliran on 11/3/2015.
 */
public class FilterBuilderProvider {

    public static FilterBuilder getFilter(ElasticSearchStep searchStep){
        return getFilter(searchStep,null);
    }
    public static FilterBuilder getFilter(ElasticSearchStep searchStep,Direction direction){
        Object[] ids = searchStep.getIds();
        List<HasContainer> hasContainers = searchStep.getPredicates();
        if (hasContainers.size() == 0 && ids.length == 0) return null;
        BoolFilterBuilder boolFilterBuilder = FilterBuilders.boolFilter();
        for (HasContainer has : hasContainers) {
            if (has.predicate instanceof Compare) {
                String predicateString = has.predicate.toString();
                switch (predicateString) {
                    case ("eq"):
                        if (has.key.equals("~label")) searchStep.setTypeLabel(has.value.toString());
                        else if(has.key.equals("~id")) {
                            searchStep.clearIds();
                            searchStep.addId(has.value);
                            ids = searchStep.getIds();
                        }
                        else boolFilterBuilder.must(FilterBuilders.termFilter(has.key, has.value));
                        break;
                    case ("neq"):
                        boolFilterBuilder.mustNot(FilterBuilders.termFilter(has.key, has.value));
                        break;
                    case ("gt"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gt(has.value));
                        break;
                    case ("gte"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).gte(has.value));
                        break;
                    case ("lt"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lt(has.value));
                        break;
                    case ("lte"):
                        boolFilterBuilder.must(FilterBuilders.rangeFilter(has.key).lte(has.value));
                        break;
                    default:
                        throw new IllegalArgumentException("predicate not supported in has step: " + has.predicate.toString());
                }
            } else if (has.predicate instanceof Contains) {
                if (has.predicate == Contains.without)
                    boolFilterBuilder.mustNot(FilterBuilders.existsFilter(has.key));
                else if (has.predicate == Contains.within)
                    boolFilterBuilder.must(FilterBuilders.existsFilter(has.key));
            } else if (has.predicate instanceof Geo)
                boolFilterBuilder.must(new GeoShapeFilterBuilder(has.key, GetShapeBuilder(has.value), ((Geo) has.predicate).getRelation()));
            else throw new IllegalArgumentException("predicate not supported: " + has.predicate.toString());
        }
        if (ids.length > 0) {
            String[] stringIds = new String[ids.length];
            for(int i = 0; i<ids.length; i++)
                stringIds[i] = ids[i].toString();
            if(direction==null)
                boolFilterBuilder.must(FilterBuilders.idsFilter().addIds(stringIds));
            else createIdsFilterForEdgeStep(stringIds,direction, boolFilterBuilder);
        }
        if (!boolFilterBuilder.hasClauses()) return null;
        return boolFilterBuilder;
    }

    private static void createIdsFilterForEdgeStep(String[] stringIds, Direction direction, BoolFilterBuilder boolFilterBuilder) {

        TermsFilterBuilder inFilter =  FilterBuilders.termsFilter(ElasticEdge.InId, stringIds);
        TermsFilterBuilder outFilter =  FilterBuilders.termsFilter(ElasticEdge.OutId, stringIds);

        if(direction == Direction.IN) boolFilterBuilder.must(inFilter);
        else if(direction == Direction.OUT) boolFilterBuilder.must(outFilter);
        else if(direction == Direction.BOTH) boolFilterBuilder.should(inFilter, outFilter);
        else throw new EnumConstantNotPresentException(direction.getClass(),direction.name());
    }

    private static ShapeBuilder GetShapeBuilder(Object object) {
        try {
            String geoJson = (String) object;
            XContentParser parser = JsonXContent.jsonXContent.createParser(geoJson);
            parser.nextToken();

            return ShapeBuilder.parse(parser);
        } catch (Exception e) {
            return null;
        }
    }



}
