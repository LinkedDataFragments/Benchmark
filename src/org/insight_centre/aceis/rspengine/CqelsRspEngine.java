package org.insight_centre.aceis.rspengine;

import com.hp.hpl.jena.reasoner.ReasonerRegistry;
import org.deri.cqels.engine.ContinuousSelect;
import org.deri.cqels.engine.ExecContext;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.io.EventRepository;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.cqels.*;
import org.insight_centre.citybench.main.CityBench;

import java.util.List;
import java.util.Map;

/**
 * RSP engine declaration for CQELS
 * @author Ruben Taelman
 */
public class CqelsRspEngine extends RspEngine {

    public static ExecContext cqelsContext, tempContext;

    public CqelsRspEngine() {
        super("cqels");
    }

    @Override
    public void init(String dataset) {
        // initialize datasets
        try {
            tempContext = RDFFileManager.initializeCQELSContext(dataset, ReasonerRegistry.getRDFSReasoner());
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
                CQELSSensorStream css;
                EventDeclaration ed = cityBench.er.getEds().get(uri);
                if (ed == null)
                    throw new Exception("ED not found for: " + uri);
                if (ed.getEventType().contains("traffic")) {
                    css = new CQELSAarhusTrafficStream(cqelsContext, uri, path, ed, cityBench.start, cityBench.end);
                } else if (ed.getEventType().contains("pollution")) {
                    css = new CQELSAarhusPollutionStream(cqelsContext, uri, path, ed, cityBench.start, cityBench.end);
                } else if (ed.getEventType().contains("weather")) {
                    css = new CQELSAarhusWeatherStream(cqelsContext, uri, path, ed, cityBench.start, cityBench.end);
                } else if (ed.getEventType().contains("location"))
                    css = new CQELSLocationStream(cqelsContext, uri, path, ed);
                else if (ed.getEventType().contains("parking"))
                    css = new CQELSAarhusParkingStream(cqelsContext, uri, path, ed, cityBench.start, cityBench.end);
                else
                    throw new Exception("Sensor type not supported: " + ed.getEventType());
                css.setRate(cityBench.rate);
                css.setFreq(cityBench.frequency);
                new Thread(css).start();
                cityBench.startedStreamObjects.add(css);
            }
        }
    }

    @Override
    public void startTests(CityBench cityBench, Map<String, String> queryMap, int queryDuplicates) throws Exception{
        cqelsContext = tempContext;
        for (int i = 0; i < queryDuplicates; i++)
            this.registerQueries(cityBench, queryMap);
        this.startStreams(cityBench, queryMap);
    }

    @Override
    public void registerQuery(CityBench cityBench, String qid, String query) {
        if (!cityBench.registeredQueries.keySet().contains(qid)) {
            CQELSResultListener crl = new CQELSResultListener(qid);
            logger.info("Registering result observer: " + crl.getUri());
            ContinuousSelect cs = cqelsContext.registerSelect(query);
            cs.register(crl);
            cityBench.registeredQueries.put(qid, crl);
        }

    }
}
