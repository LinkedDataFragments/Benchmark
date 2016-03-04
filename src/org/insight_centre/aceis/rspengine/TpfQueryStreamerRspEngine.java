package org.insight_centre.aceis.rspengine;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.util.FileManager;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.querystreamer.*;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.citybench.main.CityBench;

import java.io.*;
import java.text.ParseException;
import java.util.*;

/**
 * RSP engine declaration for the TPF Query Streamer
 * @author Ruben Taelman
 */
public class TpfQueryStreamerRspEngine extends RspEngine {

    private QueryStreamerEndpoint endpoint;
    private Process proxyProcess;
    private Process serverProcess;

    private String tpfStreamingExec = "/Users/kroeser/Documents/School/Thesis/TPFStreamingQueryExecutor/";
    private String ldfServerPath = tpfStreamingExec + "node_modules/ldf-server/";
    private String ldfServerBin = "bin/ldf-server";
    private int insertPort = 4000;
    private String target = "http://localhost:3001/train";

    private boolean debug = true;
    private String type = "graphs";
    private boolean interval = false;
    private boolean caching = false;

    public static Set<String> capturedObIds = Collections.newSetFromMap(Maps.newConcurrentMap());
    public static Set<String> capturedResults = Collections.newSetFromMap(Maps.newConcurrentMap());
    private static int serverPid = -1;
    private static final List<Integer> clientPids = Lists.newLinkedList();

    public TpfQueryStreamerRspEngine() {
        super("querystreamer");
    }

    @Override
    public void init(String dataset) {
        // Load properties
        Properties prop = new Properties();
        try {
            prop.load(new FileInputStream("querystreamer.properties"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        tpfStreamingExec = prop.getProperty("tpfStreamingExec");
        ldfServerPath = prop.getProperty("ldfServerPath");
        ldfServerBin = prop.getProperty("ldfServerBin");
        insertPort = Integer.parseInt(prop.getProperty("insertPort"));
        target = prop.getProperty("target");

        debug = Boolean.parseBoolean(prop.getProperty("debug"));
        type = prop.getProperty("type");
        interval = Boolean.parseBoolean(prop.getProperty("interval"));
        caching = Boolean.parseBoolean(prop.getProperty("caching"));

        // Start the actual LDF server
        startLdfServer();

        // Our connection to the LDF server
        endpoint = new QueryStreamerEndpoint(insertPort);

        // initialize datasets
        try {
            Model defaultModel = FileManager.get().loadModel(RDFFileManager.datasetDirectory + dataset);
            Model ces = FileManager.get().loadModel(RDFFileManager.ontologyDirectory + "ces.n3");
            Model culturalevents = FileManager.get().loadModel(RDFFileManager.datasetDirectory + "AarhusCulturalEvents.n3");
            Model libraryevents = FileManager.get().loadModel(RDFFileManager.datasetDirectory + "AarhusLibraryEvents.n3");

            RDFFileManager.dataset = DatasetFactory.create(defaultModel);
            RDFFileManager.dataset.addNamedModel(RDFFileManager.cesPrefix, ces);
            RDFFileManager.dataset.addNamedModel("culturalevents", culturalevents);
            RDFFileManager.dataset.addNamedModel("libraryevents", libraryevents);

            endpoint.insertStaticData(RDFFileManager.dataset);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    protected boolean startLdfServer() {
        Map<String, String> env = Maps.newHashMap();
        if(debug) env.put("DEBUG", "true");
        env.put("TYPE", type);
        env.put("SERVER", ldfServerPath + ldfServerBin);
        env.put("INTERVAL", Boolean.toString(interval));
        env.put("INSERTPORT", Integer.toString(insertPort));
        env.put("TARGET", target);

        // Start the proxy between our client and server
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(tpfStreamingExec + "bin/http-proxy");
            if(debug) {
                processBuilder.inheritIO();
            }
            proxyProcess = processBuilder.start();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        // Setup the LDF server with updating data
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("node ldf-server-http-inserter config_citybench.json".split(" "));
            processBuilder.directory(new File(tpfStreamingExec + "bin/"));
            if(debug) {
                processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
            }
            processBuilder.environment().putAll(env);
            serverProcess = processBuilder.start();
            new Thread(new ServerObserver(serverProcess, debug)).start();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    protected List<String> getQueryStreams(CityBench cityBench, String query) throws Exception {
        List<String> streams = Lists.newLinkedList();
        for (String line : query.split("\\n")) {
            if (line.length() > 0 && line.charAt(0) == '#') {
                streams.add(line.substring(1));
            }
        }
        return streams;
    }

    @Override
    public void startTests(CityBench cityBench, Map<String, String> queryMap, int queryDuplicates) throws Exception {
        this.startStreams(cityBench, queryMap);
        for (int i = 0; i < queryDuplicates; i++)
            this.registerQueries(cityBench, queryMap);
    }

    @Override
    public Object constructStream(String type, String uri, String path, EventDeclaration ed, Date start, Date end, double rate, double frequency) throws Exception {
        QueryStreamerSensorStream qss;
        if (ed == null)
            throw new Exception("ED not found for: " + uri);
        if (type.contains("traffic")) {
            qss = new QueryStreamerAarhusTrafficStream(endpoint, uri, path, ed, start, end);
        } else if (type.contains("pollution")) {
            qss = new QueryStreamerAarhusPollutionStream(endpoint, uri, path, ed, start, end);
        } else if (type.contains("weather")) {
            qss = new QueryStreamerAarhusWeatherStream(endpoint, uri, path, ed, start, end);
        } else if (type.contains("location")) {
            qss = new QueryStreamerLocationStream(endpoint, uri, path, ed);
        } else if (type.contains("parking")) {
            qss = new QueryStreamerAarhusParkingStream(endpoint, uri, path, ed, start, end);
        }
        else
            throw new Exception("Sensor type not supported: " + ed.getEventType());
        qss.setRate(rate);
        qss.setFreq(frequency);
        new Thread(qss).start();
        return qss;
    }

    @Override
    public void registerQuery(CityBench cityBench, String qid, String query) throws ParseException {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("node", "querymeta", type);
            processBuilder.directory(new File(tpfStreamingExec + "bin/"));
            if(debug) {
                processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
            }

            Map<String, String> env = Maps.newHashMap();
            if(debug) env.put("DEBUG", "true");
            env.put("QUERY", query);
            env.put("CACHING", Boolean.toString(caching));
            env.put("TARGET", target);

            processBuilder.environment().putAll(env);
            Process queryProcess = processBuilder.start();
            new Thread(new ResultObserver(queryProcess, qid)).start();

            cityBench.registeredQueries.put(qid, queryProcess);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy(CityBench cityBench) {
        // Stop queries
        for(Object q : cityBench.registeredQueries.values()) {
            ((Process) q).destroyForcibly();
        }

        // Stop streams
        for (Object css : CityBench.startedStreamObjects) {
            ((QueryStreamerSensorStream) css).stop();
        }

        // Stop server
        serverProcess.destroyForcibly();
        proxyProcess.destroyForcibly();
    }

    @Override
    public long getServerMemoryUsage() {
        return super.getServerMemoryUsage() + getProcessStats(serverPid).getMemory();
    }

    @Override
    public long getClientMemoryUsage() {
        long total = 0;
        List<Integer> pids;
        synchronized (clientPids) {
            pids = Lists.newArrayList(clientPids);
        }
        for(int clientPid : pids) {
            total += getProcessStats(clientPid).getMemory();
        }
        return total;
    }

    @Override
    public double getClientCpu() {
        double total = 0;
        List<Integer> pids;
        synchronized (clientPids) {
            pids = Lists.newArrayList(clientPids);
        }
        for(int clientPid : pids) {
            total += getProcessStats(clientPid).getCpu();
        }
        return total;
    }

    public static class ServerObserver implements Runnable {

        private final Process serverProccess;
        private final boolean debug;

        public ServerObserver(Process serverProccess, boolean debug) {
            this.serverProccess = serverProccess;
            this.debug = debug;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(serverProccess.getInputStream()));
            String result;
            try {
                while ((result = reader.readLine()) != null) {
                    if (result.contains("$PID=")) {
                        serverPid = Integer.parseInt(result.substring("$PID=".length()));
                    } else if(debug) {
                        System.out.println(result);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    public static class ResultObserver implements Runnable {

        private final Process queryProcess;
        private final String qid;

        public ResultObserver(Process queryProcess, String qid) {
            this.queryProcess = queryProcess;
            this.qid = qid;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(queryProcess.getInputStream()));
            String result;
            try {
                while ((result = reader.readLine()) != null) {
                    if (result.contains("$PID=")) {
                        synchronized (clientPids) {
                            clientPids.add(Integer.parseInt(result.substring("$PID=".length())));
                        }
                    } else if (result.contains("$RESULT=")) {
                        Map<String, Long> latencies = Maps.newHashMap();
                        Map<String, String> data = new Gson().fromJson(result.substring("$RESULT=".length()), new TypeToken<Map<String, String>>(){}.getType());
                        for(Map.Entry<String, String> entry : data.entrySet()) {
                            String obid = entry.getValue();
                            if (obid == null)
                                logger.error("NULL ob Id detected.");
                            if(CityBench.obMap.containsKey(obid)) {
                                if (!capturedObIds.contains(obid)) {
                                    capturedObIds.add(obid);
                                    try {
                                        SensorObservation so = CityBench.obMap.get(obid);
                                        if (so == null)
                                            logger.error("Cannot find observation for: " + obid);
                                        long creationTime = so.getSysTimestamp().getTime();
                                        latencies.put(obid, (System.currentTimeMillis() - creationTime));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            }
                        }

                        if(!latencies.isEmpty()) {
                            if (!capturedResults.contains(result)) {
                                capturedResults.add(result);
                                CityBench.pm.addResults(qid, latencies, 1);
                            } else {
                                logger.debug("Query Streamer result discarded: " + result);
                            }
                        }
                    } else {
                        System.out.println(result);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return super.toString() + "$" + String.format("type:%s;interval:%s;caching:%s;", type, interval, caching);
    }
}
