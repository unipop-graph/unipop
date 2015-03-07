package com.tinkerpop.gremlin.elastic.tools;

import org.apache.commons.lang3.time.StopWatch;

import java.text.DecimalFormat;
import java.util.HashMap;

public class TimingAccessor {

    private HashMap<String, Timer> timers = new HashMap<>();
    static DecimalFormat twoDForm = new DecimalFormat("#.##");

    public Timer timer(String name) {
        if (timers.containsKey(name)) return timers.get(name);
        Timer timer = new Timer(name);
        timers.put(name, timer);
        return timer;
    }

    public void print() {
        timers.values().forEach((timer) -> timer.PrintStats());
    }

    public class Timer {

        private String name;
        StopWatch sw = new StopWatch();
        float numOfRuns = 0;
        float longestRun = 0;
        private long lastRun = 0;

        public Timer(String name) {
            this.name = name;
            sw.reset();
        }

        public void start() {
            if (sw.isSuspended()) sw.resume();
            else sw.start();
        }

        public void stop() {
            sw.suspend();
            long time = sw.getTime() - lastRun;
            if (time > longestRun) longestRun = time;
            lastRun = time;
            numOfRuns++;
        }

        public void PrintStats() {
            if (numOfRuns > 0) {
                float time = sw.getTime() / 1000f;

                System.out.println(name + ": " + twoDForm.format(time) + " total secs, " + twoDForm.format(time / numOfRuns) + " secs per run, " + numOfRuns + " runs, " + twoDForm.format(longestRun / 1000f) + " sec for longest run");
            }
        }
    }
}
