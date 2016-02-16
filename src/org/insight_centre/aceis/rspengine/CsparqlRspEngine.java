package org.insight_centre.aceis.rspengine;

import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import eu.larkc.csparql.engine.CsparqlEngineImpl;
import eu.larkc.csparql.engine.CsparqlQueryResultProxy;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.csparql.*;
import org.insight_centre.citybench.main.CityBench;

import java.text.ParseException;
import java.util.List;
import java.util.Map;

/**
 * RSP engine declaration for C-SPARQL
 * @author Ruben Taelman
 */
public class CsparqlRspEngine extends RspEngine {

    public static CsparqlEngineImpl csparqlEngine;

    public CsparqlRspEngine() {
        super("csparql");
    }

    @Override
    public void init(String dataset) {
        // initialize datasets
        try {
            RDFFileManager.initializeCSPARQLContext(dataset, ReasonerRegistry.getRDFSReasoner());
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(0);
        }
    }

    @Override
    public void startStreamsFromQuery(CityBench cityBench, String query) throws Exception {
        List<String> streamNames = cityBench.getStreamFileNamesFromQuery(query);
        for (String sn : streamNames) {
            String uri = RDFFileManager.defaultPrefix + sn.split("\\.")[0];
            String path = cityBench.streams + "/" + sn;
            if (!cityBench.startedStreams.contains(uri)) {
                cityBench.startedStreams.add(uri);
                CSPARQLSensorStream css;
                EventDeclaration ed = cityBench.er.getEds().get(uri);
                if (ed.getEventType().contains("traffic")) {
                    css = new CSPARQLAarhusTrafficStream(uri, path, ed, cityBench.start, cityBench.end);
                } else if (ed.getEventType().contains("pollution")) {
                    css = new CSPARQLAarhusPollutionStream(uri, path, ed, cityBench.start, cityBench.end);
                } else if (ed.getEventType().contains("weather")) {
                    css = new CSPARQLAarhusWeatherStream(uri, path, ed, cityBench.start, cityBench.end);
                } else if (ed.getEventType().contains("location"))
                    css = new CSPARQLLocationStream(uri, path, ed);
                else if (ed.getEventType().contains("parking"))
                    css = new CSPARQLAarhusParkingStream(uri, path, ed, cityBench.start, cityBench.end);
                else
                    throw new Exception("Sensor type not supported.");
                css.setRate(cityBench.rate);
                css.setFreq(cityBench.frequency);
                csparqlEngine.registerStream(css);
                new Thread(css).start();
                cityBench.startedStreamObjects.add(css);
            }
        }
    }

    @Override
    public void startTests(CityBench cityBench, Map<String, String> queryMap, int queryDuplicates) throws Exception {
        csparqlEngine = new CsparqlEngineImpl();
        csparqlEngine.initialize(true);
        this.startStreams(cityBench, queryMap);
        for (int i = 0; i < queryDuplicates; i++)
            this.registerQueries(cityBench, queryMap);
    }

    @Override
    public String transformQuery(String queryId, String query) {
        return "REGISTER QUERY " + queryId + " AS " + query;
    }

    @Override
    public void registerQuery(CityBench cityBench, String qid, String query) throws ParseException {
        if (!cityBench.registeredQueries.keySet().contains(qid)) {
            CsparqlQueryResultProxy cqrp = csparqlEngine.registerQuery(query);
            CSPARQLResultObserver cro = new CSPARQLResultObserver(qid);
            logger.info("Registering result observer: " + cro.getIRI());
            csparqlEngine.registerStream(cro);

            // RDFStreamFormatter cro = new RDFStreamFormatter(streamURI);
            cqrp.addObserver(cro);
            cityBench.registeredQueries.put(qid, cro);
        }
    }
}
