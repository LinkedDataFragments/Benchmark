package org.insight_centre.aceis.rspengine;

import com.google.common.collect.Maps;
import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.querystreamer.QueryStreamerAarhusTrafficStream;
import org.insight_centre.aceis.io.streams.querystreamer.QueryStreamerEndpoint;
import org.insight_centre.aceis.io.streams.querystreamer.QueryStreamerSensorStream;
import org.insight_centre.citybench.main.CityBench;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

/**
 * RSP engine declaration for the TPF Query Streamer
 * TODO: correctly handle CPU/MEM measurements
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

        // Start the actual LDF server
        startLdfServer();

        // Our connection to the LDF server
        endpoint = new QueryStreamerEndpoint(insertPort);

        // initialize datasets
        try {
            RDFFileManager.initializeCSPARQLContext(dataset, ReasonerRegistry.getRDFSReasoner());
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
                processBuilder.inheritIO();
            }
            processBuilder.environment().putAll(env);
            serverProcess = processBuilder.start();
        } catch (IOException e) {
            e.printStackTrace();
            return false;
        }

        return true;
    }

    @Override
    protected List<String> getQueryStreams(CityBench cityBench, String query) throws Exception {
        // TODO: abstract as multimap query->streams
        List<String> streams = new LinkedList<>();
        streams.add("AarhusTrafficData182955.stream");
        streams.add("AarhusTrafficData158505.stream");
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
        }/* else if (type.contains("pollution")) {
            css = new CQELSAarhusPollutionStream(cqelsContext, uri, path, ed, start, end);
        } else if (type.contains("weather")) {
            css = new CQELSAarhusWeatherStream(cqelsContext, uri, path, ed, start, end);
        } else if (type.contains("location"))
            css = new CQELSLocationStream(cqelsContext, uri, path, ed);
        else if (type.contains("parking"))
            css = new CQELSAarhusParkingStream(cqelsContext, uri, path, ed, start, end);*/
        else
            throw new Exception("Sensor type not supported: " + ed.getEventType());
        qss.setRate(rate);
        qss.setFreq(frequency);
        new Thread(qss).start();
        return qss;
    }

    @Override
    public void registerQuery(CityBench cityBench, String qid, String query) throws ParseException {
        // TODO: start a query streamer process for the given query
    }

    @Override
    public void destroy(CityBench cityBench) {
        // Stop streams
        for (Object css : CityBench.startedStreamObjects) {
            ((QueryStreamerSensorStream) css).stop();
        }

        // Stop server
        serverProcess.destroyForcibly();
        proxyProcess.destroyForcibly();
    }
}
