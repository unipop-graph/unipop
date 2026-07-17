package org.unipop.jdbc.utils;

import org.apache.commons.lang3.time.StopWatch;
import org.javatuples.Pair;
import org.jooq.ExecuteContext;
import org.jooq.impl.DefaultExecuteListener;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by sbarzilay on 8/2/16.
 */
public class TimingExecuterListener extends DefaultExecuteListener {
    public static Map<String, Pair<Long, Integer>> timing = new HashMap<>();

    /**
     * Key by the query rendered inlined under the executing context's dialect. Consumers
     * (RowController.fillChildren) reconstruct the same key via ContextManager.renderInlined(select);
     * ctx.query().toString() cannot be reproduced by a consumer because it renders formatted
     * (multi-line) and against the query's own (possibly default-dialect) configuration.
     */
    private static String key(ExecuteContext ctx) {
        return ctx.dsl().renderInlined(ctx.query());
    }

    @Override
    public void fetchEnd(ExecuteContext ctx) {
        super.fetchEnd(ctx);

        Pair<Long, Integer> stopWatchIntegerPair = timing.get(key(ctx));
        long duration = System.nanoTime() - stopWatchIntegerPair.getValue0();
        timing.put(key(ctx), new Pair<>(duration, ctx.result().size()));
    }

    @Override
    public void fetchStart(ExecuteContext ctx) {
        super.start(ctx);
        StopWatch stopWatch = new StopWatch();
        timing.put(key(ctx), new Pair<>(System.nanoTime(), 0));
        stopWatch.start();
    }
}
