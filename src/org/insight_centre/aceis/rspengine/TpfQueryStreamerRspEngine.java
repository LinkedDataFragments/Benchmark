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
import java.net.URL;
import java.net.URLConnection;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

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
    private String target = "http://localhost:3001/citybench";

    private boolean debug = true;
    private String type = "graphs";
    private boolean interval = false;
    private boolean caching = false;
    private static Map<String, List<ClientRegistration>> clientPool = Maps.newHashMap();
    private String clientsUsername;
    private String clientsPassword;
    private int workers;

    public static Set<String> capturedObIds = Collections.newSetFromMap(Maps.newConcurrentMap());
    public static Set<String> capturedResults = Collections.newSetFromMap(Maps.newConcurrentMap());
    private static List<Integer> serverPids = Lists.newArrayList();

    private static final Map<ClientRegistration, ProcessStatter> remoteProcessStatters = Maps.newConcurrentMap();
    private static final Map<ClientRegistration, ProcessStats> lastRemoteProcessStats = Maps.newConcurrentMap();
    private static final Set<ClientRegistration> nullRemoteProcessStats = ConcurrentHashMap.newKeySet();

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
        clientPool = Lists.newArrayList(prop.getProperty("client-pool").split(" ")).stream()
                .collect(Collectors.toMap((c) -> c, (c) -> Lists.newLinkedList()));
        logger.info("Client pool: " + clientPool);
        clientsUsername = prop.getProperty("clients-username");
        clientsPassword = prop.getProperty("clients-password");
        workers = Integer.parseInt(prop.getProperty("workers"));

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

    protected ClientRegistration reserveClient() {
        String host = null;
        int count = 0;
        for (Map.Entry<String, List<ClientRegistration>> entry : clientPool.entrySet()) {
            if(entry.getValue().size() < count || host == null) {
                host = entry.getKey();
                count = entry.getValue().size();
            }
        }
        if(host == null) {
            throw new RuntimeException("Could not reserve a client in the pool: " + clientPool);
        }
        ClientRegistration clientRegistration = new ClientRegistration(host, count);
        clientPool.get(host).add(clientRegistration);
        return clientRegistration;
    }

    protected boolean startLdfServer() {
        Map<String, String> env = Maps.newHashMap();
        if(debug) env.put("DEBUG", "true");
        env.put("TYPE", type);
        env.put("SERVER", ldfServerPath + ldfServerBin);
        env.put("INTERVAL", Boolean.toString(interval));
        env.put("INSERTPORT", Integer.toString(insertPort));
        env.put("TARGET", target);
        env.put("PROXYBINSFILE", "result_log/" + CityBench.getResultName() + "-proxybins.json");
        env.put("WORKERS", Integer.toString(workers));

        // Start the proxy between our client and server
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(tpfStreamingExec + "bin/http-proxy");
            if(debug) {
                processBuilder.inheritIO();
            }
            processBuilder.environment().putAll(env);
            proxyProcess = processBuilder.start();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        // Setup the LDF server with updating data
        try {
            ProcessBuilder processBuilder = new ProcessBuilder(("node ldf-server-http-inserter config_citybench.json 3000 " + workers).split(" "));
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
            ClientRegistration registration = reserveClient();
            clientPool.get(registration.host).add(registration);
            logger.info(String.format("Registered query %s on %s as %s", qid, registration.host, registration.localId));
            ProcessBuilder processBuilder = new ProcessBuilder("./start_remote_client.sh",
                    registration.host,
                    clientsUsername,
                    clientsPassword,
                    Boolean.toString(debug),
                    type,
                    Boolean.toString(interval),
                    Boolean.toString(caching),
                    query,
                    target,
                    Integer.toString(registration.localId));
            processBuilder.directory(new File("bin/"));
            if(debug) {
                processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
            } else {
                processBuilder.redirectErrorStream(true);
            }
            Process queryProcess = processBuilder.start();
            new Thread(new ResultObserver(queryProcess, qid, registration, debug)).start();

            cityBench.registeredQueries.put(qid, queryProcess);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void stopRemoteClient(ClientRegistration registration) {
        try {
            ProcessBuilder processBuilder = new ProcessBuilder("./stop_remote_client.sh",
                    registration.host,
                    clientsUsername,
                    clientsPassword,
                    Integer.toString(registration.localId));
            processBuilder.directory(new File("bin/"));
            if(debug) {
                processBuilder.redirectError(ProcessBuilder.Redirect.INHERIT);
            } else {
                processBuilder.redirectErrorStream(true);
            }
            Process stopProcess = processBuilder.start();
            stopProcess.waitFor();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void destroy(CityBench cityBench) {
        super.destroy(cityBench);
        try {
            // Stop queries
            for (Object q : cityBench.registeredQueries.values()) {
                ((Process) q).destroyForcibly().waitFor();
            }

            for (ClientRegistration registration : getClientRegistrations()) {
                stopRemoteClient(registration);
            }


            // Stop streams
            for (Object css : CityBench.startedStreamObjects) {
                ((QueryStreamerSensorStream) css).stop();
            }

            // Stop server
            writeProxyBins();
            serverProcess.destroyForcibly().waitFor();
            proxyProcess.destroyForcibly().waitFor();
        } catch(InterruptedException e) {}
    }

    private void writeProxyBins() {
        try {
            String closeProxy = target + "/closeProxy";
            URL url = new URL(closeProxy);
            URLConnection connection = url.openConnection();
            BufferedReader in = new BufferedReader(new InputStreamReader(connection.getInputStream()));
            while (in.readLine() != null); // Make sure we capture all output, and only close after that.
            in.close();
        } catch (IOException e) {
            // Ignore errors, because an error will be thrown once the proxy closes, which is what we want.
        }
    }

    @Override
    public long getServerMemoryUsage() {
        long total = 0;
        for(int serverPid : serverPids) {
            total += getProcessStats(serverPid).getMemory();
        }
        System.out.println("SERVER WORKER COUNT " + serverPids.size()); // TODO
        if(total == 0) {
            return 0;
        }
        return total;
    }

    @Override
    public double getServerCpu() {
        long total = 0;
        for(int serverPid : serverPids) {
            total += getProcessStats(serverPid).getCpu();
        }
        if(total == 0) {
            return 0;
        }
        return total;
    }

    protected List<ClientRegistration> getClientRegistrations() {
        List<ClientRegistration> ret = Lists.newLinkedList();
        Map<String, List<ClientRegistration>> clientPoolCopy;
        synchronized (clientPool) {
            clientPoolCopy = Maps.newHashMap(clientPool);
        }
        for (Map.Entry<String, List<ClientRegistration>> entry : clientPoolCopy.entrySet()) {
            List<ClientRegistration> registrationsCopy;
            synchronized (entry.getValue()) {
                registrationsCopy = Lists.newArrayList(entry.getValue());
            }
            for (ClientRegistration registration : registrationsCopy) {
                if (registration.pid >= 0 ) {
                    ret.add(registration);
                }
            }
        }
        return ret;
    }

    @Override
    public long getClientMemoryUsage() {
        long total = 0;
        int count = 0;
        for(ClientRegistration registration : getClientRegistrations()) {
            total += getRemoteProcessStats(registration).getMemory();
            count++;
        }
        if(total == 0) {
            return 0;
        }
        return total / count;
    }

    @Override
    public double getClientCpu() {
        double total = 0;
        int count = 0;
        for(ClientRegistration registration : getClientRegistrations()) {
            total += getRemoteProcessStats(registration).getCpu();
            count++;
        }
        if(total == 0) {
            return 0;
        }
        return total / count;
    }

    protected ProcessStats getRemoteProcessStats(ClientRegistration clientRegistration) {
        if(!remoteProcessStatters.containsKey(clientRegistration)) {
            RemoteProcessStatter statter = new RemoteProcessStatter(clientsUsername, clientsPassword, clientRegistration);
            remoteProcessStatters.put(clientRegistration, statter);
            new Thread(statter).start();
        }
        long end_at = System.currentTimeMillis() + 100; // Wait a maximum of 100 ms on the results, will be repeated during next iteration.
        while(!nullRemoteProcessStats.contains(clientRegistration) && !lastRemoteProcessStats.containsKey(clientRegistration) && System.currentTimeMillis() < end_at) {
            Thread.yield();
        }
        ProcessStats processStats = lastRemoteProcessStats.get(clientRegistration);
        if(processStats == null) {
            processStats = new ProcessStats(0, 0);
        }
        return processStats;
    }

    protected static class RemoteProcessStatter extends ProcessStatter {

        private final ClientRegistration clientRegistration;
        private final String username;
        private final String password;

        RemoteProcessStatter(String username, String password, ClientRegistration clientRegistration) {
            super(clientRegistration.pid);
            this.username = username;
            this.password = password;
            this.clientRegistration = clientRegistration;
        }

        @Override
        public void onFail() {
            nullRemoteProcessStats.add(clientRegistration);
        }

        @Override
        public void onSuccess(ProcessStats stats) {
            lastRemoteProcessStats.put(clientRegistration, stats);
        }

        @Override
        protected ProcessBuilder getProcessBuilder() {
            ProcessBuilder processBuilder = new ProcessBuilder("./top_remote_client.sh",
                    clientRegistration.host,
                    username,
                    password,
                    Integer.toString(clientRegistration.localId),
                    Integer.toString(clientRegistration.pid));
            processBuilder.directory(new File("bin/"));
            processBuilder.redirectErrorStream(true);
            return processBuilder;
        }
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
                        serverPids.add(Integer.parseInt(result.substring("$PID=".length())));
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
        private final ClientRegistration clientRegistration;
        private final boolean debug;

        public ResultObserver(Process queryProcess, String qid, ClientRegistration clientRegistration, boolean debug) {
            this.queryProcess = queryProcess;
            this.qid = qid;
            this.clientRegistration = clientRegistration;
            this.debug = debug;
        }

        @Override
        public void run() {
            BufferedReader reader = new BufferedReader(new InputStreamReader(queryProcess.getInputStream()));
            String result;
            try {
                while ((result = reader.readLine()) != null) {
                    if (result.contains("$PID=")) {
                        clientRegistration.pid = Integer.parseInt(result.substring(result.indexOf("$PID=") + "$PID=".length()));
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
                                CityBench.pm.addResults(qid, latencies, 1, (float) capturedObIds.size() / (float) CityBench.obMap.size());
                            } else {
                                logger.debug("Query Streamer result discarded: " + result);
                            }
                        }
                    } else if(debug) {
                        System.err.println(result);
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    @Override
    public String toString() {
        return super.toString() + "_" + String.format("type=%s;interval=%s;caching=%s", type, interval, caching);
    }

    static class ClientRegistration {
        private final String host;
        private final int localId;
        private int pid;

        ClientRegistration(String host, int localId) {
            this.host = host;
            this.localId = localId;
            this.pid = -1;
        }
    }
}
