package org.insight_centre.aceis.io.streams.querystreamer;

import com.csvreader.CsvReader;
import com.hp.hpl.jena.rdf.model.*;
import com.hp.hpl.jena.vocabulary.RDF;
import org.insight_centre.aceis.eventmodel.EventDeclaration;
import org.insight_centre.aceis.eventmodel.TrafficReportService;
import org.insight_centre.aceis.io.rdf.RDFFileManager;
import org.insight_centre.aceis.io.streams.DataWrapper;
import org.insight_centre.aceis.observations.AarhusTrafficObservation;
import org.insight_centre.aceis.observations.SensorObservation;
import org.insight_centre.citybench.main.CityBench;
import org.openjena.atlas.lib.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.UUID;

//import com.hp.hpl.jena.graph.Node;

public class QueryStreamerAarhusTrafficStream extends QueryStreamerSensorStream {
	private static final Logger logger = LoggerFactory.getLogger(QueryStreamerAarhusTrafficStream.class);
	EventDeclaration ed;
	private Date startDate = null, endDate = null;
	String distance;
	CsvReader streamData, metaData;
	String txtFile;

	public QueryStreamerAarhusTrafficStream(QueryStreamerEndpoint endpoint, String uri, String txtFile, EventDeclaration ed, Date start, Date end)
			throws IOException {
		super(endpoint, uri);
		this.startDate = start;
		this.endDate = end;
		this.txtFile = txtFile;
		this.ed = ed;
		streamData = new CsvReader(String.valueOf(txtFile));
		streamData.setTrimWhitespace(false);
		streamData.setDelimiter(',');
		streamData.readHeaders();
		metaData = new CsvReader("dataset/MetaData/trafficMetaData.csv");
		metaData.readHeaders();
		streamData.readRecord();
		while (metaData.readRecord()) {
			if (streamData.get("REPORT_ID").equals(metaData.get("REPORT_ID"))) {
				distance = metaData.get("DISTANCE_IN_METERS");
				metaData.close();
				break;
			}
		}
	}

	@Override
	protected SensorObservation createObservation(Object objData) {
		try {
			AarhusTrafficObservation data = new AarhusTrafficObservation(Double.parseDouble(streamData.get("REPORT_ID")),
					Double.parseDouble(streamData.get("avgSpeed")), Double.parseDouble(streamData.get("vehicleCount")),
					Double.parseDouble(streamData.get("avgMeasuredTime")), 0, 0, null, null, 0.0, 0.0, null, null, 0.0,
					0.0, null, null, streamData.get("TIMESTAMP"));
			String obId = "AarhusTrafficObservation-" + streamData.get("_id");
			Double distance = Double.parseDouble(((TrafficReportService) ed).getDistance() + "");
			if (data.getAverageSpeed() != 0)
				data.setEstimatedTime(distance / data.getAverageSpeed());
			else
				data.setEstimatedTime(-1.0);
			if (distance != 0)
				data.setCongestionLevel(data.getVehicle_count() / distance);
			else
				data.setCongestionLevel(-1.0);
			data.setObId(obId);
			DataWrapper.waitForInterval(this.currentObservation, data, this.startDate, getRate());
			this.currentObservation = data;
			return data;
		} catch (NumberFormatException | IOException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	protected List<IdentifiableStatement> getStatements(SensorObservation data) throws NumberFormatException, IOException {
		List<IdentifiableStatement> statements = new LinkedList<>();
		if (ed != null)
			for (String pStr : ed.getPayloads()) {
				Model m = ModelFactory.createDefaultModel();
				String obId = data.getObId();
				Resource observation = m.createResource(RDFFileManager.defaultPrefix + obId + UUID.randomUUID());
				CityBench.obMap.put(observation.toString(), data);
				observation.addProperty(RDF.type, m.createResource(RDFFileManager.ssnPrefix + "Observation"));

				Resource serviceID = m.createResource(ed.getServiceId());
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedBy"), serviceID);
				observation.addProperty(m.createProperty(RDFFileManager.ssnPrefix + "observedProperty"),
						m.createResource(pStr.split("\\|")[2]));
				Property hasValue = m.createProperty(RDFFileManager.saoPrefix + "hasValue");
				if (pStr.contains("AvgSpeed"))
					observation.addLiteral(hasValue, ((AarhusTrafficObservation) data).getAverageSpeed());
				else if (pStr.contains("VehicleCount")) {
					double value = ((AarhusTrafficObservation) data).getVehicle_count();
					observation.addLiteral(hasValue, value);
				} else if (pStr.contains("MeasuredTime"))
					observation.addLiteral(hasValue, ((AarhusTrafficObservation) data).getAvgMeasuredTime());
				else if (pStr.contains("EstimatedTime"))
					observation.addLiteral(hasValue, ((AarhusTrafficObservation) data).getEstimatedTime());
				else if (pStr.contains("CongestionLevel"))
					observation.addLiteral(hasValue, ((AarhusTrafficObservation) data).getCongestionLevel());
				statements.add(new IdentifiableStatement(ed.getServiceId().split("#")[1], m.listStatements().toList()));
			}
		return statements;
	}

	public void run() {
		logger.info("Starting sensor stream: " + this.getUri() + " " + this.startDate + ", " + this.endDate);
		try {
			// Reads csv document for traffic metadata
			boolean completed = false;
			int cnt = 0;
			while (streamData.readRecord() && !stop) {
				Date obTime = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss").parse(streamData.get("TIMESTAMP"));
				logger.debug("Reading data: " + streamData.toString());
				if (this.startDate != null && this.endDate != null) {
					if (obTime.before(this.startDate) || obTime.after(this.endDate)) {
						logger.debug(this.getUri() + ": Disgarded observation @" + obTime);
						continue;
					}
				}
				AarhusTrafficObservation data = (AarhusTrafficObservation) this.createObservation(streamData);
				List<IdentifiableStatement> stmts = this.getStatements(data);
				long messageByte = 0;
				cnt += 1;
				// uncomment for testing the completeness, i.e., restrict the observations produced
				// if (cnt >= 2)
				// completed = true;
				try {
					if (completed) {
						logger.info("My mission completed: " + this.getUri());
						Thread.sleep(sleep);
						continue;
					}

				} catch (InterruptedException e) {

					e.printStackTrace();

				}
				long timeInitial = System.currentTimeMillis();
				long timeFinal = System.currentTimeMillis() + sleep;
				for (IdentifiableStatement is : stmts) {
					for (Statement st : is.statements) {
						getEndpoint().stream(st.getSubject(), st.getPredicate(), st.getObject());
						// logger.info(this.getURI() + " Streaming: " + st.toString());
						messageByte += st.toString().getBytes().length;
					}
					getEndpoint().flush(timeInitial, timeFinal, is.id);
				}
				if (sleep > 0) {
					try {
						if (this.getRate() == 1.0)
							Thread.sleep(sleep);
					} catch (InterruptedException e) {

						e.printStackTrace();

					}
				}
			}
		} catch (Exception e) {

			e.printStackTrace();
		} finally {
			logger.info("Stream Terminated: " + this.getUri());
			this.stop();

		}
	}

	public Date getEndDate() {
		return endDate;
	}

	public void setEndDate(Date endDate) {
		this.endDate = endDate;
	}

	public Date getStartDate() {
		return startDate;
	}

	public void setStartDate(Date startDate) {
		this.startDate = startDate;
	}

}