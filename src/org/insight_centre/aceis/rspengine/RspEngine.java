package org.insight_centre.aceis.rspengine;

import com.google.common.collect.Maps;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RSP engine declaration.
 * @author Ruben Taelman
 */
public abstract class RspEngine {

    protected static final Logger logger = LoggerFactory.getLogger(CityBench.class);

    private static final Map<Long, ProcessStatter> processStatters = Maps.newConcurrentMap();
    private static final Map<Long, ProcessStats> lastProcessStats = Maps.newConcurrentMap();
    private static final Set<Long> nullProcessStats = ConcurrentHashMap.newKeySet();

    private final String id;
    private String queryDirectory = null;

    public RspEngine(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public void setQueryDirectory(String queryDirectory) {
        this.queryDirectory = queryDirectory;
    }

    public String getQueryDirectory() {
        return this.queryDirectory;
    }

    public String transformQuery(String queryId, String query) {
        return query;
    }

    protected List<String> getQueryStreams(CityBench cityBench, String query) throws Exception {
        return cityBench.getStreamFileNamesFromQuery(query);
    }

    public void startStreamsFromQuery(CityBench cityBench, String query) throws Exception {
        List<String> streamNames = getQueryStreams(cityBench, query);
        for (String sn : streamNames) {
            String uri = RDFFileManager.defaultPrefix + sn.split("\\.")[0];
            String path = cityBench.streams + "/" + sn;
            if (!cityBench.startedStreams.contains(uri)) {
                cityBench.startedStreams.add(uri);
                EventDeclaration ed = cityBench.er.getEds().get(uri);
                cityBench.startedStreamObjects.add(constructStream(ed.getEventType(), uri, path, ed, cityBench.start, cityBench.end, cityBench.rate, cityBench.frequency));
            }
        }
    }

    protected void startStreams(CityBench cityBench, Map<String, String> queryMap) throws Exception{
        for (String s : queryMap.values()) {
            this.startStreamsFromQuery(cityBench, s);
        }
    }

    protected void registerQueries(CityBench cityBench, Map<String, String> queryMap) throws ParseException {
        for (Map.Entry en : queryMap.entrySet()) {
            String qid = en.getKey() + "-" + UUID.randomUUID();
            String query = en.getValue() + "";
            registerQuery(cityBench, qid, query);
        }
    }

    abstract public void init(String dataset);
    abstract public void startTests(CityBench cityBench, Map<String, String> queryMap, int queryDuplicates) throws Exception;
    abstract public Object constructStream(String type, String uri, String path, EventDeclaration ed, Date start, Date end, double rate, double frequency) throws Exception;
    abstract public void registerQuery(CityBench cityBench, String qid, String query) throws ParseException;
    public void destroy(CityBench cityBench) {
        processStatters.values().forEach(ProcessStatter::stopProcess);
    }

    protected static ProcessStats getProcessStats(long pid) {
        if(pid < 0) {
            throw new IllegalArgumentException("Pid must be >= 0, got " + pid);
        }
        if(!processStatters.containsKey(pid)) {
            ProcessStatter statter = new ProcessStatter(pid);
            processStatters.put(pid, statter);
            new Thread(statter).start();
        }
        long end_at = System.currentTimeMillis() + 100; // Wait a maximum of 100 ms on the results, will be repeated during next iteration.
        while(!nullProcessStats.contains(pid) && !lastProcessStats.containsKey(pid) && System.currentTimeMillis() < end_at) {
            Thread.yield();
        }
        ProcessStats processStats = lastProcessStats.get(pid);
        if(processStats == null) {
            processStats = new ProcessStats(0, 0);
        }
        return processStats;
    }

    public static long getPid() {
        String processName = ManagementFactory.getRuntimeMXBean().getName();
        return Long.parseLong(processName.split("@")[0]);
    }

    /**
     * @return The current server memory usage (in bytes).
     */
    public long getServerMemoryUsage() {
        Runtime rt = Runtime.getRuntime();
        return rt.totalMemory() - rt.freeMemory();
        //return getProcessStats(getPid()).getMemory();
    }

    /**
     * @return The percentage of cpu usage the (RSP) server currently uses.
     */
    public double getServerCpu() {
        return getProcessStats(getPid()).getCpu();
    }

    /**
     * @return The current client memory usage (in bytes).
     */
    public long getClientMemoryUsage() {
        return 0;
    }

    /**
     * @return The percentage of cpu usage all (RSP) clients currently use.
     */
    public double getClientCpu() {
        return 0;
    }

    static class ProcessStats {

        private final double cpu;
        private final long memory;

        ProcessStats(double cpu, long memory) {
            this.cpu = cpu;
            this.memory = memory;
        }

        public long getMemory() {
            return memory;
        }

        public double getCpu() {
            return cpu;
        }
    }

    protected static class ProcessStatter implements Runnable {

        private final long pid;
        private Process process;

        ProcessStatter(long pid) {
            this.pid = pid;
        }

        protected ProcessBuilder getProcessBuilder() {
            ProcessBuilder processBuilder = new ProcessBuilder("./top_pid.sh", String.valueOf(pid));
            processBuilder.directory(new File("bin/"));
            processBuilder.redirectErrorStream(true);
            return processBuilder;
        }

        public void onSuccess(ProcessStats stats) {
            lastProcessStats.put(pid, stats);
        }

        public void onFail() {
            nullProcessStats.add(pid);
        }

        @Override
        public void run() {
            ProcessBuilder processBuilder = getProcessBuilder();
            try {
                this.process = processBuilder.start();
                BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
                String line;
                while ((line = reader.readLine()) != null) {
                    if(line.contains("Error")) {
                        System.err.println("Could not top " + pid);
                        onFail();
                    } else {
                        String[] split = line.split(" +");
                        int multiplier = 1;
                        if (split[2].contains("K")) multiplier = 1024;
                        if (split[2].contains("M")) multiplier = 1024 * 1024;
                        if (split[2].contains("G")) multiplier = 1024 * 1024 * 1024;
                        ProcessStats stats = new ProcessStats(
                                Double.parseDouble(split[1]), Integer.parseInt(split[2].replaceAll("[^0-9]*", "")) * multiplier);
                        onSuccess(stats);
                    }
                }
            } catch (NumberFormatException | IOException e) {}
            onFail();
        }

        public void stopProcess() {
            if(this.process != null) {
                try {
                    this.process.destroyForcibly().waitFor();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

    }

    @Override
    public String toString() {
        return getId();
    }
}
