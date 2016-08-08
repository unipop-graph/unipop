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

    @Override
    public void fetchEnd(ExecuteContext ctx) {
        super.fetchEnd(ctx);

        Pair<Long, Integer> stopWatchIntegerPair = timing.get(ctx.query().toString());
        long duration = System.nanoTime() - stopWatchIntegerPair.getValue0();
        timing.put(ctx.query().toString(), new Pair<>(duration, ctx.result().size()));
    }

    @Override
    public void fetchStart(ExecuteContext ctx) {
        super.start(ctx);
        StopWatch stopWatch = new StopWatch();
        timing.put(ctx.query().toString(), new Pair<>(System.nanoTime(), 0));
        stopWatch.start();
    }
}
