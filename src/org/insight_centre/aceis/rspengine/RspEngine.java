package org.insight_centre.aceis.rspengine;

import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.citybench.main.CityBench;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
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

    abstract public void init(String dataset);
    abstract public void startStreamsFromQuery(CityBench cityBench, String query) throws Exception;
    abstract public void registerQuery(CityBench cityBench, String qid, String query) throws ParseException;
    abstract public void startTests(CityBench cityBench, Map<String, String> queryMap, int queryDuplicates) throws Exception;

    public String transformQuery(String queryId, String query) {
        return query;
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
}
