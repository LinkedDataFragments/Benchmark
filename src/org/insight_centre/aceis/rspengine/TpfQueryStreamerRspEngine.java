package org.insight_centre.aceis.rspengine;

import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.querystreamer.QueryStreamerAarhusTrafficStream;
import org.insight_centre.aceis.io.streams.querystreamer.QueryStreamerEndpoint;
import org.insight_centre.aceis.io.streams.querystreamer.QueryStreamerSensorStream;
import org.insight_centre.citybench.main.CityBench;

import java.text.ParseException;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * @author Ruben Taelman
 */
public class TpfQueryStreamerRspEngine extends RspEngine {

    private QueryStreamerEndpoint endpoint;

    public TpfQueryStreamerRspEngine() {
        super("querystreamer");
    }

    @Override
    public void init(String dataset) {
        // TODO: find node files for client and server and set ready to run
        endpoint = new QueryStreamerEndpoint();

        // initialize datasets
        try {
            RDFFileManager.initializeCSPARQLContext(dataset, ReasonerRegistry.getRDFSReasoner());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
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
        // TODO: start the server (no data should be inserted yet)
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
        // TODO: stop server
        for (Object css : CityBench.startedStreamObjects) {
            ((QueryStreamerSensorStream) css).stop();
        }
    }
}
