package org.unipop.elastic.common;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.searchbox.client.JestClient;
import io.searchbox.client.JestResult;
import io.searchbox.client.http.JestHttpClient;
import io.searchbox.core.Search;
import io.searchbox.core.SearchScroll;
import io.searchbox.params.Parameters;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.jooq.lambda.Seq;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

public class QueryIterator<E extends Element> implements Iterator<E> {

    private JestResult scrollResponse;
    private String scrollId;
    private final Parser<E> parser;
    private final int scrollSize;
    private JestClient client;
    private Iterator<JsonElement> hits;
    private final Gson gson;

    public QueryIterator(QueryBuilder query, int scrollSize, int maxSize, JestClient client,
                         Parser<E> parser, String... indices) throws IOException {
        this.scrollSize = scrollSize;
        this.client = client;
        this.parser = parser;

        this.gson = new Gson();

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(query);

        Search.Builder searchBuilder = new Search.Builder(searchSourceBuilder.toString())
                .addIndex(Seq.of(indices).toList());

        if(maxSize == -1) maxSize = 10000;

        if (scrollSize > 0) {
            searchBuilder.setParameter(Parameters.SCROLL, new TimeValue(60000));
            searchBuilder.setParameter(Parameters.SIZE, Math.min(maxSize, scrollSize));
        }

        else {
            searchBuilder.setParameter(Parameters.SIZE, maxSize);
        }

        Search search = searchBuilder.build();

        this.scrollResponse = client.execute(search);
        this.hits = this.scrollResponse.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits").iterator();
    }

    @Override
    public boolean hasNext() {
        if (hits.hasNext()) return true;

        if (scrollSize > 0) {
            try {
                SearchScroll scroll = new SearchScroll.Builder(
                        this.scrollResponse.getJsonObject().getAsJsonPrimitive("_scroll_id").getAsString(),
                        new TimeValue(60000).format())
                        .build();
                this.scrollResponse = client.execute(scroll);
                this.hits = scrollResponse.getJsonObject().getAsJsonObject("hits").getAsJsonArray("hits").iterator();
            } catch (IOException e) {
                // TODO: Handle Exception
            }
        }

        return hits.hasNext();
    }

    @Override
    public E next() {
        Map<String, Object> hit = this.gson.<Map<String, Object>>fromJson(hits.next(), Map.class);
        return parser.parse(hit);
    }

    public interface Parser<E> {
        E parse(Map<String, Object> hit);
    }
}
