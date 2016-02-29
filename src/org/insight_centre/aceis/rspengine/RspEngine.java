package org.insight_centre.aceis.rspengine;

import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * RSP engine declaration.
 * @author Ruben Taelman
 */
public abstract class RspEngine {

    protected static final Logger logger = LoggerFactory.getLogger(CityBench.class);

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
    abstract public void destroy(CityBench cityBench);

    /**
     * @return The additional memory usage that should be taken into account for this RSP engine (in bytes).
     */
    public int getExternalMemoryUsage() {
        return 0;
    }

}
