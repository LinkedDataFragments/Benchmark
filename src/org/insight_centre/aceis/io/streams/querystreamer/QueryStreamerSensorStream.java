package org.insight_centre.aceis.io.streams.querystreamer;

import com.hp.hpl.jena.rdf.model.Statement;
import org.insight_centre.aceis.observations.SensorObservation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class QueryStreamerSensorStream implements Runnable {
	protected Logger logger = LoggerFactory.getLogger(getClass());

    private final QueryStreamerEndpoint endpoint;
	private final String uri;
	protected double rate = 1.0;
	// private int sleep = 1000;
	protected int sleep = 1000;
	protected boolean stop = false;
	protected SensorObservation currentObservation;
	protected List<String> requestedProperties = new ArrayList<>();

	public QueryStreamerSensorStream(QueryStreamerEndpoint endpoint, String uri) {
        this.endpoint = endpoint;
		this.uri = uri;
	}

	public List<String> getRequestedProperties() {
		return requestedProperties;
	}

	public void setRequestedProperties(List<String> requestedProperties) {
		this.requestedProperties = requestedProperties;
	}

	public void setRate(Double rate) {
		this.rate = rate;
		if (this.rate != 1.0)
			logger.info("Streaming acceleration rate set to: " + rate);
	}

	public double getRate() {
		return rate;
	}

	public void setFreq(Double freq) {
		sleep = (int) (sleep / freq);
		if (this.rate == 1.0)
			logger.info("Streaming interval set to: " + sleep + " ms");
	}

	public void stop() {
		if (!stop) {
			stop = true;
			logger.info("Stopping stream: " + this.uri);
		}
	}

	protected abstract List<IdentifiableStatement> getStatements(SensorObservation so) throws NumberFormatException, IOException;

	protected abstract SensorObservation createObservation(Object data);

	public SensorObservation getCurrentObservation() {
		return this.currentObservation;
	}

    protected String getUri() {
        return this.uri;
    }

    public QueryStreamerEndpoint getEndpoint() {
        return endpoint;
    }

    public static class IdentifiableStatement {

        public final String id;
        public final List<Statement> statements;

        public IdentifiableStatement(String id, List<Statement> statements) {
            this.id = id;
            this.statements = statements;
        }
    }
}
